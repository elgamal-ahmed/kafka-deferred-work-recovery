package com.elgamal.redecide.naive;

import com.elgamal.redecide.IntegrationTest;
import com.elgamal.redecide.events.DomainEvent.BeneficiaryActivated;
import com.elgamal.redecide.events.DomainEvent.PaymentCancelled;
import com.elgamal.redecide.events.DomainEvent.PaymentRequested;
import com.elgamal.redecide.events.EventPublisher;
import com.elgamal.redecide.support.EventJournal.Kind;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;

import static com.elgamal.redecide.payment.PaymentStatus.SUBMITTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Problem 2a: @RetryableTopic keeps the partition moving by sending the failed record to a retry
 * topic. The business consequence is that later events for the same payment can overtake it.
 *
 * <p>Plenty of attempts, so the request is still being retried when the payee is verified.
 */
@TestPropertySource(properties = {"demo.design=retry-topic", "demo.retry-topic.attempts=30"})
class NonBlockingRetryTest extends IntegrationTest {

    @Autowired
    EventPublisher publisher;

    @Test
    void cancelOvertakesRetriedRequestAndIsLost() {
        publisher.publish(new PaymentRequested("P2", "B1", gbp("200.00")));
        await().atMost(Duration.ofSeconds(30))
                .until(() -> journal.contains(Kind.BENEFICIARY_NOT_ACTIVE, "P2"));

        // The request is now on a retry topic. The cancel is processed on the main topic first,
        // and the request's rolled-back transaction left nothing to cancel.
        publisher.publish(new PaymentCancelled("P2"));
        await().atMost(Duration.ofSeconds(30))
                .until(() -> journal.contains(Kind.SKIPPED_UNKNOWN_PAYMENT, "P2"));

        publisher.publish(new BeneficiaryActivated("B1"));

        // The retry succeeds and the cancelled payment goes out.
        await().atMost(Duration.ofSeconds(30)).until(() -> status("P2") == SUBMITTED);
        assertThat(outbox.rowsFor("P2")).hasSize(1);
    }
}
