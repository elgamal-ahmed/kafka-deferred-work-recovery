package com.elgamal.redecide.redecide;

import java.math.BigDecimal;

/**
 * Every possible result of deciding what to do with a payment. Sealed, so a switch over it
 * must handle every case: adding an outcome is a compile error at every place that applies one.
 */
public sealed interface ProcessingOutcome {

    /** The payment can go ahead now, for this amount. */
    record Execute(BigDecimal amount) implements ProcessingOutcome {
    }

    /** The payment is valid but cannot go ahead yet. It waits until the condition is met. */
    record Wait(WakeCondition until) implements ProcessingOutcome {
    }

    /** Something that happened in the meantime has made this decision unnecessary. */
    record Supersede(String reason) implements ProcessingOutcome {
    }

    /** The payment can never go ahead. */
    record Reject(String reason) implements ProcessingOutcome {
    }
}
