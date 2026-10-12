package com.elgamal.payments.waiting;

import java.math.BigDecimal;

/**
 * Every possible result of deciding what to do with a payment. Sealed, so a switch over it
 * must handle every case: adding an outcome is a compile error at every place that applies one.
 */
public sealed interface Decision {

    /** The payment can go ahead now, for this amount. */
    record Execute(BigDecimal amount) implements Decision {
    }

    /** The payment is valid but cannot go ahead until this beneficiary is activated. */
    record Wait(String beneficiaryId) implements Decision {
    }

    /** Something that happened in the meantime has made this decision unnecessary. */
    record Supersede(String reason) implements Decision {
    }
}
