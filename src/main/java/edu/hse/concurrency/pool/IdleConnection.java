package edu.hse.concurrency.pool;

record IdleConnection(RawConnection raw, long since) {
}
