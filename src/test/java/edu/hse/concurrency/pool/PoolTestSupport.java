package edu.hse.concurrency.pool;

import java.time.Duration;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertTrue;

final class PoolTestSupport {
    static final Duration WAIT = Duration.ofSeconds(5);

    private PoolTestSupport() {
    }

    static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertTrue(condition.getAsBoolean(), "Условие не выполнено за 5 секунд");
    }
}
