package com.elgamal.redecide.redecide;

import com.elgamal.redecide.beneficiary.BeneficiaryStatus;
import com.elgamal.redecide.payment.Payment;
import com.elgamal.redecide.redecide.ProcessingOutcome.Execute;
import com.elgamal.redecide.redecide.ProcessingOutcome.Supersede;
import com.elgamal.redecide.redecide.ProcessingOutcome.Wait;

/**
 * The one domain decision. It runs on first delivery and again on every wake-up, always against
 * current state. It never looks at the original message, so an amendment or cancellation that
 * happened while the payment waited is taken into account automatically.
 *
 * <p>Pure: no I/O, no framework. Everything it needs is passed in.
 */
public class PaymentDecider {

    public ProcessingOutcome decide(Payment payment, BeneficiaryStatus beneficiary) {
        return switch (payment.status()) {
            case SUBMITTED, CANCELLED -> new Supersede("payment is already " + payment.status());
            case REQUESTED, WAITING_FOR_BENEFICIARY -> beneficiary == BeneficiaryStatus.ACTIVE
                    ? new Execute(payment.amount())
                    : new Wait(WakeCondition.beneficiaryActivated(payment.beneficiaryId()));
        };
    }
}
