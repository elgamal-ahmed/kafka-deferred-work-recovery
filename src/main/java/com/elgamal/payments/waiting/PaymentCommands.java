package com.elgamal.payments.waiting;

import com.elgamal.payments.beneficiary.BeneficiaryProjection;
import com.elgamal.payments.beneficiary.BeneficiaryStatus;
import com.elgamal.payments.events.DomainEvent.PaymentAmended;
import com.elgamal.payments.events.DomainEvent.PaymentCancelled;
import com.elgamal.payments.events.DomainEvent.PaymentRequested;
import com.elgamal.payments.payment.Payment;
import com.elgamal.payments.payment.PaymentRepository;
import com.elgamal.payments.support.EventJournal;
import com.elgamal.payments.support.EventJournal.Kind;
import com.elgamal.payments.support.Outbox;
import com.elgamal.payments.support.ProcessingHooks;
import com.elgamal.payments.waiting.Decision.Execute;
import com.elgamal.payments.waiting.Decision.Supersede;
import com.elgamal.payments.waiting.Decision.Wait;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Entry points for the waiting design. A missing business precondition never throws:
 * the request and its waiting state commit together.
 */
@Service
@ConditionalOnProperty(name = "demo.design", havingValue = "waiting")
public class PaymentCommands {

    private static final int MAX_ATTEMPTS = 3;

    private static final String WAITING_FOR_BENEFICIARY = """
            select id from payment
            where beneficiary_id = ? and status = 'WAITING_FOR_BENEFICIARY'
            order by id
            """;

    private final TransactionTemplate tx;
    private final PaymentRepository payments;
    private final BeneficiaryProjection beneficiaries;
    private final PaymentDecider decider;
    private final Outbox outbox;
    private final JdbcTemplate jdbc;
    private final EventJournal journal;
    private final ProcessingHooks hooks;

    public PaymentCommands(TransactionTemplate tx,
                           PaymentRepository payments,
                           BeneficiaryProjection beneficiaries,
                           PaymentDecider decider,
                           Outbox outbox,
                           JdbcTemplate jdbc,
                           EventJournal journal,
                           ProcessingHooks hooks) {
        this.tx = tx;
        this.payments = payments;
        this.beneficiaries = beneficiaries;
        this.decider = decider;
        this.outbox = outbox;
        this.jdbc = jdbc;
        this.journal = journal;
        this.hooks = hooks;
    }

    /** Save the request and decide in one transaction. Empty if this request was already received. */
    public Optional<Decision> accept(PaymentRequested event) {
        return tx.execute(status -> {
            if (payments.existsById(event.paymentId())) {
                return Optional.<Decision>empty();
            }
            Payment payment = payments.save(
                    Payment.requested(event.paymentId(), event.beneficiaryId(), event.amount()));
            Decision decision = decideAndApply(payment);
            payments.flush();
            return Optional.of(decision);
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
     * Record the activation, then find who was waiting for it, then decide again on each waiting
     * payment in its own transaction.
     *
     * <p>Relies on READ COMMITTED. If a deferral holds the beneficiary row, the upsert waits for
     * it to commit, and the select that follows takes a new snapshot, so it sees the payment the
     * deferral saved. Under REPEATABLE READ the select would reuse the snapshot from before the
     * wait and miss that payment.
     */
    public Map<String, Decision> onBeneficiaryActivated(String beneficiaryId) {
        List<String> waiting = tx.execute(status -> {
            beneficiaries.activate(beneficiaryId);
            return jdbc.queryForList(WAITING_FOR_BENEFICIARY, String.class, beneficiaryId);
        });
        return decideAgainAll(waiting);
    }

    /**
     * Run the domain decision again against current state. If another transaction changed the
     * payment after we loaded it, the version check fails and we decide again on fresh state.
     */
    public Decision decideAgain(String paymentId) {
        for (int attempt = 1; ; attempt++) {
            try {
                return tx.execute(status -> {
                    Payment payment = payments.findById(paymentId).orElseThrow();
                    Decision decision = decideAndApply(payment);
                    payments.flush();
                    return decision;
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
     * Backstop for anything event-driven waking missed. Deciding again on a payment that still
     * has to wait changes nothing.
     */
    public Map<String, Decision> sweep() {
        List<String> waiting = jdbc.queryForList(
                "select id from payment where status = 'WAITING_FOR_BENEFICIARY' order by id", String.class);
        return decideAgainAll(waiting);
    }

    /** Reads current state, asks the decider, and applies the decision. Runs inside the caller's transaction. */
    private Decision decideAndApply(Payment payment) {
        BeneficiaryStatus beneficiary = beneficiaries.readForDecision(payment.beneficiaryId());
        hooks.afterPreconditionRead(payment.id());

        Decision decision = decider.decide(payment, beneficiary);
        switch (decision) {
            case Execute(BigDecimal amount) -> {
                payment.submit();
                outbox.paymentSubmitted(payment.id(), amount);
            }
            case Wait w -> payment.markWaiting();
            case Supersede s -> journal.record(Kind.SUPERSEDED, payment.id());
        }
        return decision;
    }

    /**
     * A payment that is still contended after every attempt stays waiting, so it does not stop the
     * others; the next wake-up or sweep decides it again. Any other failure propagates, so Kafka
     * retries the record.
     */
    private Map<String, Decision> decideAgainAll(List<String> paymentIds) {
        Map<String, Decision> decisions = new LinkedHashMap<>();
        for (String paymentId : paymentIds) {
            try {
                decisions.put(paymentId, decideAgain(paymentId));
            } catch (OptimisticLockingFailureException e) {
                journal.record(Kind.GAVE_UP_AFTER_CONFLICTS, paymentId);
            }
        }
        return decisions;
    }
}
