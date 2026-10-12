package com.elgamal.redecide.redecide;

import com.elgamal.redecide.beneficiary.BeneficiaryProjection;
import com.elgamal.redecide.beneficiary.BeneficiaryStatus;
import com.elgamal.redecide.payment.Payment;
import com.elgamal.redecide.redecide.ProcessingOutcome.Execute;
import com.elgamal.redecide.redecide.ProcessingOutcome.Supersede;
import com.elgamal.redecide.redecide.ProcessingOutcome.Wait;
import com.elgamal.redecide.support.EventJournal;
import com.elgamal.redecide.support.EventJournal.Kind;
import com.elgamal.redecide.support.Outbox;
import com.elgamal.redecide.support.ProcessingHooks;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * Reads current state, asks the decider, and applies the outcome. Must run inside a transaction.
 */
@Component
@ConditionalOnProperty(name = "demo.design", havingValue = "redecide")
public class PaymentProcessor {

    private final BeneficiaryProjection beneficiaries;
    private final PaymentDecider decider;
    private final Outbox outbox;
    private final EventJournal journal;
    private final ProcessingHooks hooks;

    public PaymentProcessor(BeneficiaryProjection beneficiaries,
                            PaymentDecider decider,
                            Outbox outbox,
                            EventJournal journal,
                            ProcessingHooks hooks) {
        this.beneficiaries = beneficiaries;
        this.decider = decider;
        this.outbox = outbox;
        this.journal = journal;
        this.hooks = hooks;
    }

    public ProcessingOutcome evaluate(Payment payment) {
        BeneficiaryStatus beneficiary = beneficiaries.readForDecision(payment.beneficiaryId());
        hooks.afterPreconditionRead(payment.id());

        ProcessingOutcome outcome = decider.decide(payment, beneficiary);
        switch (outcome) {
            case Execute(BigDecimal amount) -> {
                payment.submit();
                outbox.paymentSubmitted(payment.id(), amount);
            }
            case Wait(WakeCondition until) -> payment.markWaiting();
            case Supersede(String reason) -> journal.record(Kind.SUPERSEDED, payment.id());
        }
        return outcome;
    }
}
