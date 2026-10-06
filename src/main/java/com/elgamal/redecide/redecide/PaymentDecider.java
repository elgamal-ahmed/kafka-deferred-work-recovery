package com.elgamal.redecide.redecide;

import com.elgamal.redecide.beneficiary.BeneficiaryStatus;
import com.elgamal.redecide.payment.Payment;
import com.elgamal.redecide.redecide.ProcessingOutcome.Execute;
import com.elgamal.redecide.redecide.ProcessingOutcome.Reject;
import com.elgamal.redecide.redecide.ProcessingOutcome.Supersede;
import com.elgamal.redecide.redecide.ProcessingOutcome.Wait;

import java.time.Duration;
import java.time.Instant;

/**
 * The one domain decision. It runs on first delivery and again on every wake-up, always against
 * current state. It never looks at the original message, so an amendment or cancellation that
 * happened while the payment waited is taken into account automatically.
 *
 * <p>Pure: no I/O, no clock, no framework. Everything it needs is passed in.
 */
public class PaymentDecider {

    private final Duration waitTimeout;

    public PaymentDecider(Duration waitTimeout) {
        this.waitTimeout = waitTimeout;
    }

    public ProcessingOutcome decide(Payment payment, BeneficiaryStatus beneficiary, Instant now) {
        return switch (payment.status()) {
            case SUBMITTED, CANCELLED, REJECTED -> new Supersede("payment is already " + payment.status());
            case REQUESTED, WAITING_FOR_BENEFICIARY -> decideOpenPayment(payment, beneficiary, now);
        };
    }

    private ProcessingOutcome decideOpenPayment(Payment payment, BeneficiaryStatus beneficiary, Instant now) {
        if (beneficiary == BeneficiaryStatus.ACTIVE) {
            return new Execute(payment.amount());
        }
        if (payment.waitingSince() != null && payment.waitingSince().plus(waitTimeout).isBefore(now)) {
            return new Reject("beneficiary not verified within " + waitTimeout);
        }
        return new Wait(WakeCondition.beneficiaryActivated(payment.beneficiaryId()));
    }
}
