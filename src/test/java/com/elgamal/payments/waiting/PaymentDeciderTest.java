package com.elgamal.payments.waiting;

import com.elgamal.payments.payment.Payment;
import com.elgamal.payments.waiting.Decision.Execute;
import com.elgamal.payments.waiting.Decision.Supersede;
import com.elgamal.payments.waiting.Decision.Wait;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static com.elgamal.payments.beneficiary.BeneficiaryStatus.ACTIVE;
import static com.elgamal.payments.beneficiary.BeneficiaryStatus.PENDING;
import static org.assertj.core.api.Assertions.assertThat;

/** The decision is a pure function, so it is tested without Spring, Kafka or a database. */
class PaymentDeciderTest {

    private final PaymentDecider decider = new PaymentDecider();

    @Test
    void executesTheCurrentAmountNotTheOriginalOne() {
        Payment payment = Payment.requested("P1", "B1", new BigDecimal("500.00"));
        payment.markWaiting();
        payment.amend(new BigDecimal("300.00"));

        assertThat(decider.decide(payment, ACTIVE)).isEqualTo(new Execute(new BigDecimal("300.00")));
    }

    @Test
    void waitsForTheBeneficiaryToBeActivated() {
        Payment payment = Payment.requested("P1", "B1", new BigDecimal("500.00"));

        assertThat(decider.decide(payment, PENDING))
                .isEqualTo(new Wait("B1"));
    }

    @Test
    void supersedesACancelledPaymentEvenOnceTheBeneficiaryIsActive() {
        Payment payment = Payment.requested("P2", "B1", new BigDecimal("200.00"));
        payment.cancel();

        assertThat(decider.decide(payment, ACTIVE)).isInstanceOf(Supersede.class);
    }

    @Test
    void supersedesAnAlreadySubmittedPayment() {
        Payment payment = Payment.requested("P1", "B1", new BigDecimal("500.00"));
        payment.submit();

        assertThat(decider.decide(payment, ACTIVE)).isInstanceOf(Supersede.class);
    }
}
