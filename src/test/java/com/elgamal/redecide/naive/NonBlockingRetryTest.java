package com.elgamal.redecide.naive;

import com.elgamal.redecide.IntegrationTest;
import com.elgamal.redecide.beneficiary.BeneficiaryStatus;
import com.elgamal.redecide.events.DomainEvent.BeneficiaryActivated;
import com.elgamal.redecide.events.DomainEvent.PaymentAmended;
import com.elgamal.redecide.events.DomainEvent.PaymentCancelled;
import com.elgamal.redecide.events.DomainEvent.PaymentRequested;
import com.elgamal.redecide.events.EventCodec;
import com.elgamal.redecide.events.EventPublisher;
import com.elgamal.redecide.support.DeadLetters;
import com.elgamal.redecide.support.EventJournal.Kind;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;

import static com.elgamal.redecide.payment.PaymentStatus.SUBMITTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Problem 2: @RetryableTopic keeps the partition moving by sending the failed record to a retry
 * topic. The business consequence is that later events for the same payment can overtake it.
 */
@TestPropertySource(properties = "demo.design=retry-topic")
class NonBlockingRetryTest extends IntegrationTest {

    @Autowired
    EventPublisher publisher;

    @Autowired
    EventCodec codec;

    @Autowired
    DeadLetters deadLetters;

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

    @Test
    void deadLetterRedriveSendsStaleAmount() {
        publisher.publish(new PaymentRequested("P1", "B2", gbp("500.00")));
        publisher.publish(new PaymentAmended("P1", gbp("300.00")));
        await().atMost(Duration.ofSeconds(30))
                .until(() -> journal.contains(Kind.SKIPPED_UNKNOWN_PAYMENT, "P1"));

        // Retries run out before the payee is verified.
        await().atMost(Duration.ofSeconds(60)).until(() -> deadLetters.payloads().size() == 1);

        publisher.publish(new BeneficiaryActivated("B2"));
        await().atMost(Duration.ofSeconds(30))
                .until(() -> beneficiaries.read("B2") == BeneficiaryStatus.ACTIVE);

        // "The issue is fixed", so an operator redrives the dead letter: the original record, as it was.
        publisher.publish(codec.decode(deadLetters.payloads().getFirst()));

        await().atMost(Duration.ofSeconds(30)).until(() -> status("P1") == SUBMITTED);
        assertThat(outbox.rowsFor("P1")).hasSize(1);
        assertThat(outbox.rowsFor("P1").getFirst().amount()).isEqualByComparingTo("500.00");
    }
}
