package com.elgamal.redecide.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-memory record of things worth seeing during the demo. Entries survive transaction rollbacks,
 * which is what lets the tests count failed attempts.
 */
@Component
public class EventJournal {

    public enum Kind {
        /** A payment attempt found its beneficiary not active (naive designs). */
        BENEFICIARY_NOT_ACTIVE,
        /** An amend or cancel arrived for a payment the service has no record of. */
        SKIPPED_UNKNOWN_PAYMENT,
        /** A re-decision found the payment already cancelled or submitted. */
        SUPERSEDED,
        /** A re-decision lost an optimistic-lock race and was retried against fresh state. */
        OPTIMISTIC_CONFLICT
    }

    public record Entry(Kind kind, String paymentId) {
    }

    private static final Logger log = LoggerFactory.getLogger(EventJournal.class);

    private final List<Entry> entries = new CopyOnWriteArrayList<>();

    public void record(Kind kind, String paymentId) {
        log.info("{} {}", kind, paymentId);
        entries.add(new Entry(kind, paymentId));
    }

    public long count(Kind kind, String paymentId) {
        return entries.stream().filter(e -> e.kind() == kind && e.paymentId().equals(paymentId)).count();
    }

    public boolean contains(Kind kind, String paymentId) {
        return count(kind, paymentId) > 0;
    }

    public void clear() {
        entries.clear();
    }
}
