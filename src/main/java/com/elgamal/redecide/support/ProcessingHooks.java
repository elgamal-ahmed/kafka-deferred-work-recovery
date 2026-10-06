package com.elgamal.redecide.support;

/**
 * Seam that lets tests pause a transaction between reading the precondition and writing the
 * decision, to reproduce races deterministically. It does nothing in the running application.
 */
@FunctionalInterface
public interface ProcessingHooks {

    ProcessingHooks NONE = paymentId -> {
    };

    void afterPreconditionRead(String paymentId);
}
