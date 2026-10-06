package com.elgamal.redecide;

import com.elgamal.redecide.support.ProcessingHooks;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Pauses one transaction between "read the precondition" and "write the decision" so a test can
 * run another transaction in exactly that gap.
 */
public class PausingHooks implements ProcessingHooks {

    private final AtomicReference<Pause> armed = new AtomicReference<>();

    public Pause pauseOnce(String paymentId) {
        Pause pause = new Pause(paymentId);
        armed.set(pause);
        return pause;
    }

    public void reset() {
        armed.set(null);
    }

    @Override
    public void afterPreconditionRead(String paymentId) {
        Pause pause = armed.get();
        if (pause != null && pause.paymentId.equals(paymentId) && armed.compareAndSet(pause, null)) {
            pause.holdUntilReleased();
        }
    }

    public static final class Pause {

        private final String paymentId;
        private final CountDownLatch reached = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        private Pause(String paymentId) {
            this.paymentId = paymentId;
        }

        private void holdUntilReleased() {
            reached.countDown();
            await(released, 30, "Paused transaction for " + paymentId + " was never released");
        }

        public void awaitReached() {
            await(reached, 10, "Transaction for " + paymentId + " never reached the pause point");
        }

        public void release() {
            released.countDown();
        }

        private static void await(CountDownLatch latch, long seconds, String failure) {
            try {
                if (!latch.await(seconds, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(failure);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(failure, e);
            }
        }
    }
}
