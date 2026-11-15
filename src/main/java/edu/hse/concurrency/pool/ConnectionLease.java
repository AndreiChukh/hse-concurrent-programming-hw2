package edu.hse.concurrency.pool;

final class ConnectionLease implements PooledConnection {
    private final ConnectionPoolImpl pool;
    final RawConnection connection;
    boolean active = true;

    ConnectionLease(ConnectionPoolImpl pool, RawConnection connection) {
        this.pool = pool;
        this.connection = connection;
    }

    @Override
    public RawConnection raw() {
        return pool.raw(this);
    }

    @Override
    public void close() {
        pool.release(this);
    }
}
