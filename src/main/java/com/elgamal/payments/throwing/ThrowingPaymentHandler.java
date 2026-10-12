package com.elgamal.payments.throwing;

import com.elgamal.payments.beneficiary.BeneficiaryProjection;
import com.elgamal.payments.beneficiary.BeneficiaryStatus;
import com.elgamal.payments.events.DomainEvent;
import com.elgamal.payments.events.DomainEvent.BeneficiaryActivated;
import com.elgamal.payments.events.DomainEvent.PaymentAmended;
import com.elgamal.payments.events.DomainEvent.PaymentCancelled;
import com.elgamal.payments.events.DomainEvent.PaymentRequested;
import com.elgamal.payments.payment.Payment;
import com.elgamal.payments.payment.PaymentRepository;
import com.elgamal.payments.support.EventJournal;
import com.elgamal.payments.support.EventJournal.Kind;
import com.elgamal.payments.support.Outbox;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The common approach: a missing precondition is an exception, and retry infrastructure decides
 * when to try again. Used by both the blocking and the retry-topic designs.
 *
 * <p>Saving the payment and processing it share one database transaction (a plain
 * {@code @Transactional}; no Kafka transactions, which non-blocking retries do not support).
 * Throwing rolls the transaction back, so the service keeps no record of the request.
 */
@Service
@ConditionalOnExpression("'${demo.design}' != 'waiting'")
public class ThrowingPaymentHandler {

    private final PaymentRepository payments;
    private final BeneficiaryProjection beneficiaries;
    private final Outbox outbox;
    private final EventJournal journal;

    public ThrowingPaymentHandler(PaymentRepository payments,
                               BeneficiaryProjection beneficiaries,
                               Outbox outbox,
                               EventJournal journal) {
        this.payments = payments;
        this.beneficiaries = beneficiaries;
        this.outbox = outbox;
        this.journal = journal;
    }

    /**
     * An amend or cancel for a payment we have no record of is skipped. Throwing would not help:
     * the record would be retried too, but in this design a payment is only ever committed once
     * it has been submitted, so there is never an open payment for it to act on.
     */
    @Transactional
    public void handle(DomainEvent event) {
        switch (event) {
            case PaymentRequested e -> request(e);
            case PaymentAmended e -> payments.findById(e.paymentId()).ifPresentOrElse(
                    payment -> payment.amend(e.amount()),
                    () -> journal.record(Kind.SKIPPED_UNKNOWN_PAYMENT, e.paymentId()));
            case PaymentCancelled e -> payments.findById(e.paymentId()).ifPresentOrElse(
                    Payment::cancel,
                    () -> journal.record(Kind.SKIPPED_UNKNOWN_PAYMENT, e.paymentId()));
            case BeneficiaryActivated e -> throw new IllegalArgumentException("Unexpected on payments topic: " + e);
        }
    }

    private void request(PaymentRequested event) {
        if (payments.existsById(event.paymentId())) {
            return;
        }
        Payment payment = payments.save(
                Payment.requested(event.paymentId(), event.beneficiaryId(), event.amount()));

        if (beneficiaries.read(event.beneficiaryId()) != BeneficiaryStatus.ACTIVE) {
            journal.record(Kind.BENEFICIARY_NOT_ACTIVE, event.paymentId());
            throw new BeneficiaryNotActiveException(event.beneficiaryId());
        }
        payment.submit();
        outbox.paymentSubmitted(payment.id(), payment.amount());
    }
}
