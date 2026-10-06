package com.elgamal.redecide.redecide;

import com.elgamal.redecide.payment.Payment;
import com.elgamal.redecide.redecide.ProcessingOutcome.Execute;
import com.elgamal.redecide.redecide.ProcessingOutcome.Reject;
import com.elgamal.redecide.redecide.ProcessingOutcome.Supersede;
import com.elgamal.redecide.redecide.ProcessingOutcome.Wait;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import static com.elgamal.redecide.beneficiary.BeneficiaryStatus.ACTIVE;
import static com.elgamal.redecide.beneficiary.BeneficiaryStatus.PENDING;
import static org.assertj.core.api.Assertions.assertThat;

/** The decision is a pure function, so it is tested without Spring, Kafka or a database. */
class PaymentDeciderTest {

    private static final Instant NOW = Instant.parse("2026-10-06T09:10:00Z");

    private final PaymentDecider decider = new PaymentDecider(Duration.ofHours(24));

    @Test
    void executesTheCurrentAmountNotTheOriginalOne() {
        Payment payment = Payment.requested("P1", "B1", new BigDecimal("500.00"));
        payment.markWaiting(NOW.minusSeconds(600));
        payment.amend(new BigDecimal("300.00"));

        assertThat(decider.decide(payment, ACTIVE, NOW)).isEqualTo(new Execute(new BigDecimal("300.00")));
    }

    @Test
    void waitsForTheBeneficiaryToBeActivated() {
        Payment payment = Payment.requested("P1", "B1", new BigDecimal("500.00"));

        assertThat(decider.decide(payment, PENDING, NOW))
                .isEqualTo(new Wait(WakeCondition.beneficiaryActivated("B1")));
    }

    @Test
    void supersedesACancelledPaymentEvenOnceTheBeneficiaryIsActive() {
        Payment payment = Payment.requested("P2", "B1", new BigDecimal("200.00"));
        payment.cancel();

        assertThat(decider.decide(payment, ACTIVE, NOW)).isInstanceOf(Supersede.class);
    }

    @Test
    void supersedesAnAlreadySubmittedPayment() {
        Payment payment = Payment.requested("P1", "B1", new BigDecimal("500.00"));
        payment.submit();

        assertThat(decider.decide(payment, ACTIVE, NOW)).isInstanceOf(Supersede.class);
    }

    @Test
    void rejectsAPaymentThatWaitedLongerThanTheTimeout() {
        Payment payment = Payment.requested("P1", "B1", new BigDecimal("500.00"));
        payment.markWaiting(NOW.minus(Duration.ofHours(25)));

        assertThat(decider.decide(payment, PENDING, NOW)).isInstanceOf(Reject.class);
    }
}
