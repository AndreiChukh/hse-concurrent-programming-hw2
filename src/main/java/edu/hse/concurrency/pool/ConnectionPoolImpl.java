package edu.hse.concurrency.pool;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionPoolImpl implements ConnectionPool {
    private final ConnectionFactory factory;
    private final ReentrantLock lock;
    private final Condition maintenance;
    private final Condition drained;
    private final ArrayDeque<IdleConnection> idle = new ArrayDeque<>();
    private final ArrayDeque<PoolWaiter> waiters = new ArrayDeque<>();
    private final Set<ConnectionLease> borrowed = new HashSet<>();
    private final Thread cleaner;
    private PoolConfig config;
    private int creating;
    private int closing;
    private boolean stopped;

    public ConnectionPoolImpl(PoolConfig config, ConnectionFactory factory) {
        this.config = Objects.requireNonNull(config, "config");
        this.factory = Objects.requireNonNull(factory, "factory");
        lock = new ReentrantLock(config.fair());
        maintenance = lock.newCondition();
        drained = lock.newCondition();
        cleaner = new Thread(this::cleanIdle, "connection-pool-cleaner");
        cleaner.setDaemon(true);
        cleaner.start();
    }

    @Override
    public PooledConnection borrow(Duration timeout)
            throws InterruptedException, PoolTimeoutException, ConnectionCreationException {
        long budget = nonNegativeNanos(timeout);
        long start = System.nanoTime();
        lock.lockInterruptibly();
        PoolWaiter waiter = null;
        try {
            ensureRunning();
            while (true) {
                ensureRunning();
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                if (waiter != null && System.nanoTime() - start >= budget) {
                    throw new PoolTimeoutException(timeout);
                }
                boolean turn = waiters.isEmpty() || waiters.peekFirst() == waiter;
                if ((!config.fair() || turn) && (!idle.isEmpty() || total() < config.maxSize())) {
                    if (waiter != null) {
                        waiters.remove(waiter);
                        waiter = null;
                    }
                    if (!idle.isEmpty()) {
                        ConnectionLease lease = new ConnectionLease(this, idle.removeFirst().raw());
                        borrowed.add(lease);
                        signalNext();
                        maintenance.signal();
                        return lease;
                    }
                    creating++;
                    signalNext();
                    break;
                }
                long remaining = budget - (System.nanoTime() - start);
                if (remaining <= 0) {
                    throw new PoolTimeoutException(timeout);
                }
                if (waiter == null) {
                    waiter = new PoolWaiter(lock.newCondition());
                    waiters.addLast(waiter);
                }
                waiter.ready.awaitNanos(remaining);
            }
        } finally {
            if (waiter != null) {
                waiters.remove(waiter);
                signalNext();
            }
            lock.unlock();
        }
        return createReserved();
    }

    private PooledConnection createReserved() throws ConnectionCreationException {
        RawConnection raw;
        try {
            raw = Objects.requireNonNull(factory.create(), "factory result");
        } catch (Exception failure) {
            lock.lock();
            try {
                creating--;
                signalNext();
            } finally {
                lock.unlock();
            }
            throw new ConnectionCreationException(failure);
        }
        lock.lock();
        try {
            creating--;
            if (!stopped) {
                ConnectionLease lease = new ConnectionLease(this, raw);
                borrowed.add(lease);
                return lease;
            }
            closing++;
        } finally {
            lock.unlock();
        }
        closePhysical(raw);
        throw new IllegalStateException("Пул остановлен");
    }

    RawConnection raw(ConnectionLease lease) {
        lock.lock();
        try {
            if (!lease.active) {
                throw new IllegalStateException("Соединение возвращено или закрыто");
            }
            return lease.connection;
        } finally {
            lock.unlock();
        }
    }

    void release(ConnectionLease lease) {
        boolean close;
        lock.lock();
        try {
            if (!lease.active) {
                return;
            }
            lease.active = false;
            close = stopped || total() > config.maxSize();
            borrowed.remove(lease);
            if (close) {
                closing++;
            } else {
                idle.addLast(new IdleConnection(lease.connection, System.nanoTime()));
                maintenance.signal();
            }
            signalNext();
            drained.signalAll();
        } finally {
            lock.unlock();
        }
        if (close) {
            closePhysical(lease.connection);
        }
    }

    @Override
    public void resize(int maxSize) {
        List<RawConnection> removed = new ArrayList<>();
        lock.lock();
        try {
            PoolConfig updated = config.withMaxSize(maxSize);
            ensureRunning();
            config = updated;
            while (total() > maxSize && !idle.isEmpty()) {
                removed.add(idle.removeFirst().raw());
                closing++;
            }
            signalNext();
            maintenance.signal();
        } finally {
            lock.unlock();
        }
        removed.forEach(this::closePhysical);
    }

    @Override
    public PoolConfig config() {
        lock.lock();
        try {
            return config;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public PoolStats stats() {
        lock.lock();
        try {
            return new PoolStats(total(), idle.size(), borrowed.size(), creating, waiters.size());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown(Duration grace) throws InterruptedException {
        long budget = nonNegativeNanos(grace);
        long start = System.nanoTime();
        List<RawConnection> removed = new ArrayList<>();
        lock.lock();
        try {
            if (stopped) {
                return;
            }
            stopped = true;
            for (PoolWaiter waiter : waiters) {
                waiter.ready.signal();
            }
            while (!idle.isEmpty()) {
                removed.add(idle.removeFirst().raw());
                closing++;
            }
            maintenance.signal();
        } finally {
            lock.unlock();
        }
        removed.forEach(this::closePhysical);
        removed.clear();
        boolean interrupted = Thread.interrupted();
        lock.lock();
        try {
            while (!borrowed.isEmpty() && !interrupted) {
                long remaining = budget - (System.nanoTime() - start);
                if (remaining <= 0) {
                    break;
                }
                try {
                    drained.awaitNanos(remaining);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            for (ConnectionLease lease : borrowed) {
                lease.active = false;
                removed.add(lease.connection);
                closing++;
            }
            borrowed.clear();
        } finally {
            lock.unlock();
        }
        removed.forEach(this::closePhysical);
        lock.lock();
        try {
            while (closing > 0) {
                try {
                    drained.await();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            lock.unlock();
        }
        boolean joined = false;
        while (!joined) {
            try {
                cleaner.join();
                joined = true;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (Thread.interrupted() || interrupted) {
            throw new InterruptedException();
        }
    }

    private void cleanIdle() {
        while (true) {
            List<RawConnection> expired = new ArrayList<>();
            lock.lock();
            try {
                while (!stopped && expired.isEmpty()) {
                    if (idle.size() <= config.minIdle()) {
                        maintenance.await();
                        continue;
                    }
                    long remaining = nanos(config.idleTimeout())
                            - (System.nanoTime() - idle.peekFirst().since());
                    if (remaining > 0) {
                        maintenance.awaitNanos(remaining);
                        continue;
                    }
                    long now = System.nanoTime();
                    while (idle.size() > config.minIdle()
                            && now - idle.peekFirst().since() >= nanos(config.idleTimeout())) {
                        expired.add(idle.removeFirst().raw());
                        closing++;
                    }
                    signalNext();
                }
                if (stopped) {
                    return;
                }
            } catch (InterruptedException e) {
                continue;
            } finally {
                lock.unlock();
            }
            expired.forEach(this::closePhysical);
        }
    }

    private int total() {
        return idle.size() + borrowed.size() + creating;
    }

    private void signalNext() {
        if (!stopped && !waiters.isEmpty() && (!idle.isEmpty() || total() < config.maxSize())) {
            waiters.peekFirst().ready.signal();
        }
    }

    private void ensureRunning() {
        if (stopped) {
            throw new IllegalStateException("Пул остановлен");
        }
    }

    private void closePhysical(RawConnection raw) {
        try {
            raw.close();
        } catch (Exception ignored) {
        } finally {
            lock.lock();
            try {
                closing--;
                drained.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    private static long nonNegativeNanos(Duration duration) {
        Objects.requireNonNull(duration, "duration");
        if (duration.isNegative()) {
            throw new IllegalArgumentException("Продолжительность не может быть отрицательной");
        }
        return nanos(duration);
    }

    private static long nanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }
}
