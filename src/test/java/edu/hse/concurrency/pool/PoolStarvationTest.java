package edu.hse.concurrency.pool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static edu.hse.concurrency.pool.PoolTestSupport.WAIT;
import static edu.hse.concurrency.pool.PoolTestSupport.await;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(25)
class PoolStarvationTest {
    @Test
    void nestedBorrowExhaustsPoolWithoutJvmLockDeadlock() throws Exception {
        int threads = 3;
        ConnectionPool pool = new ConnectionPoolImpl(
                new PoolConfig(0, threads, WAIT, true), TestConnection::new);
        CountDownLatch firstBorrowed = new CountDownLatch(threads);
        try (var executor = Executors.newFixedThreadPool(threads, task ->
                new Thread(task, "starvation-request"))) {
            List<Future<?>> tasks = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                tasks.add(executor.submit(() -> {
                    try (PooledConnection first = pool.borrow(WAIT)) {
                        firstBorrowed.countDown();
                        assertTrue(firstBorrowed.await(5, TimeUnit.SECONDS));
                        assertThrows(IllegalStateException.class,
                                () -> pool.borrow(Duration.ofMinutes(1)));
                        assertThrows(IllegalStateException.class, first::raw);
                    }
                    return null;
                }));
            }
            try {
                await(() -> pool.stats().waiting() == threads);
                assertEquals(new PoolStats(threads, 0, threads, 0, threads), pool.stats());
                assertTrue(tasks.stream().noneMatch(Future::isDone));
                assertNull(ManagementFactory.getThreadMXBean().findDeadlockedThreads());
            } finally {
                pool.shutdown(Duration.ZERO);
            }
            for (Future<?> task : tasks) {
                task.get(5, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdown(Duration.ZERO);
        }
    }

    @Test
    void minimumCapacityLetsEveryRequestFinish() throws Exception {
        for (int connections : new int[]{1, 2, 3, 4}) {
            int threads = 5;
            int capacity = threads * (connections - 1) + 1;
            ConnectionPool pool = new ConnectionPoolImpl(
                    new PoolConfig(0, capacity, WAIT, true), TestConnection::new);
            CountDownLatch prefixes = new CountDownLatch(threads);
            try (var executor = Executors.newFixedThreadPool(threads)) {
                List<Future<?>> tasks = new ArrayList<>();
                for (int i = 0; i < threads; i++) {
                    tasks.add(executor.submit(() -> {
                        List<PooledConnection> held = new ArrayList<>();
                        try {
                            for (int j = 1; j < connections; j++) {
                                held.add(pool.borrow(WAIT));
                            }
                            prefixes.countDown();
                            assertTrue(prefixes.await(5, TimeUnit.SECONDS));
                            held.add(pool.borrow(WAIT));
                            assertEquals(connections, held.size());
                        } finally {
                            held.forEach(PooledConnection::close);
                        }
                        return null;
                    }));
                }
                for (Future<?> task : tasks) {
                    task.get(5, TimeUnit.SECONDS);
                }
                assertEquals(0, pool.stats().borrowed());
                assertEquals(0, pool.stats().waiting());
            } finally {
                pool.shutdown(Duration.ZERO);
            }
        }
    }

}
