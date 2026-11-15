package edu.hse.concurrency.pool;

import java.util.concurrent.locks.Condition;

final class PoolWaiter {
    final Condition ready;

    PoolWaiter(Condition ready) {
        this.ready = ready;
    }
}
