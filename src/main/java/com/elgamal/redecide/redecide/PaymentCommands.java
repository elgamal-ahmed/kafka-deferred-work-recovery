package com.elgamal.redecide.redecide;

import com.elgamal.redecide.beneficiary.BeneficiaryProjection;
import com.elgamal.redecide.events.DomainEvent.PaymentAmended;
import com.elgamal.redecide.events.DomainEvent.PaymentCancelled;
import com.elgamal.redecide.events.DomainEvent.PaymentRequested;
import com.elgamal.redecide.payment.Payment;
import com.elgamal.redecide.payment.PaymentRepository;
import com.elgamal.redecide.support.EventJournal;
import com.elgamal.redecide.support.EventJournal.Kind;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Entry points for the re-decide design. A missing business precondition never throws:
 * the request is saved first, and waiting is recorded as domain state.
 */
@Service
@ConditionalOnProperty(name = "demo.design", havingValue = "redecide")
public class PaymentCommands {

    private static final int MAX_ATTEMPTS = 3;

    private static final String WAITING_FOR_BENEFICIARY = """
            select id from payment
            where beneficiary_id = ? and status = 'WAITING_FOR_BENEFICIARY'
            order by id
            """;

    private final TransactionTemplate tx;
    private final PaymentRepository payments;
    private final PaymentProcessor processor;
    private final BeneficiaryProjection beneficiaries;
    private final JdbcTemplate jdbc;
    private final EventJournal journal;

    public PaymentCommands(TransactionTemplate tx,
                           PaymentRepository payments,
                           PaymentProcessor processor,
                           BeneficiaryProjection beneficiaries,
                           JdbcTemplate jdbc,
                           EventJournal journal) {
        this.tx = tx;
        this.payments = payments;
        this.processor = processor;
        this.beneficiaries = beneficiaries;
        this.jdbc = jdbc;
        this.journal = journal;
    }

    /** Save first, then decide. Empty if this request was already received. */
    public Optional<ProcessingOutcome> accept(PaymentRequested event) {
        return tx.execute(status -> {
            if (payments.existsById(event.paymentId())) {
                return Optional.<ProcessingOutcome>empty();
            }
            Payment payment = payments.save(
                    Payment.requested(event.paymentId(), event.beneficiaryId(), event.amount()));
            ProcessingOutcome outcome = processor.evaluate(payment);
            payments.flush();
            return Optional.of(outcome);
        });
    }

    public void amend(PaymentAmended event) {
        tx.executeWithoutResult(status -> payments.findById(event.paymentId()).ifPresentOrElse(
                payment -> payment.amend(event.amount()),
                () -> journal.record(Kind.SKIPPED_UNKNOWN_PAYMENT, event.paymentId())));
    }

    public void cancel(PaymentCancelled event) {
        tx.executeWithoutResult(status -> payments.findById(event.paymentId()).ifPresentOrElse(
                Payment::cancel,
                () -> journal.record(Kind.SKIPPED_UNKNOWN_PAYMENT, event.paymentId())));
    }

    /**
     * Record the activation and find who was waiting for it in one transaction, then re-decide
     * each waiting payment in its own transaction.
     */
    public Map<String, ProcessingOutcome> onBeneficiaryActivated(String beneficiaryId) {
        List<String> waiting = tx.execute(status -> {
            beneficiaries.activate(beneficiaryId);
            return jdbc.queryForList(WAITING_FOR_BENEFICIARY, String.class, beneficiaryId);
        });
        return redecideAll(waiting);
    }

    /**
     * Run the domain decision again against current state. If another transaction changed the
     * payment after we loaded it, the version check fails and we decide again on fresh state.
     */
    public ProcessingOutcome redecide(String paymentId) {
        for (int attempt = 1; ; attempt++) {
            try {
                return tx.execute(status -> {
                    Payment payment = payments.findById(paymentId).orElseThrow();
                    ProcessingOutcome outcome = processor.evaluate(payment);
                    payments.flush();
                    return outcome;
                });
            } catch (OptimisticLockingFailureException e) {
                journal.record(Kind.OPTIMISTIC_CONFLICT, paymentId);
                if (attempt >= MAX_ATTEMPTS) {
                    throw e;
                }
            }
        }
    }

    /**
     * Backstop for anything event-driven waking missed. Re-deciding a payment that still has to
     * wait changes nothing; one that has waited too long is rejected.
     */
    public Map<String, ProcessingOutcome> sweep() {
        List<String> waiting = jdbc.queryForList(
                "select id from payment where status = 'WAITING_FOR_BENEFICIARY' order by id", String.class);
        return redecideAll(waiting);
    }

    private Map<String, ProcessingOutcome> redecideAll(List<String> paymentIds) {
        Map<String, ProcessingOutcome> outcomes = new LinkedHashMap<>();
        for (String paymentId : paymentIds) {
            outcomes.put(paymentId, redecide(paymentId));
        }
        return outcomes;
    }
}
