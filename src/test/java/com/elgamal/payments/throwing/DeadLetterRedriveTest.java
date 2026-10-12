package com.elgamal.payments.throwing;

import com.elgamal.payments.IntegrationTest;
import com.elgamal.payments.beneficiary.BeneficiaryStatus;
import com.elgamal.payments.events.DomainEvent.BeneficiaryActivated;
import com.elgamal.payments.events.DomainEvent.PaymentAmended;
import com.elgamal.payments.events.DomainEvent.PaymentRequested;
import com.elgamal.payments.events.EventCodec;
import com.elgamal.payments.events.EventPublisher;
import com.elgamal.payments.support.DeadLetters;
import com.elgamal.payments.support.EventJournal.Kind;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;

import static com.elgamal.payments.payment.PaymentStatus.SUBMITTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Problem 2b: the amendment overtakes the retried request and is dropped. Redriving the dead
 * letter later replays the original record, so the old amount is sent.
 *
 * <p>Few, short retries, so the request reaches the dead-letter topic quickly.
 */
@TestPropertySource(properties = {
        "demo.design=retry-topic", "demo.retry-topic.attempts=3", "demo.retry-topic.delay-ms=200"})
class DeadLetterRedriveTest extends IntegrationTest {

    @Autowired
    EventPublisher publisher;

    @Autowired
    EventCodec codec;

    @Autowired
    DeadLetters deadLetters;

    @Test
    void deadLetterRedriveSendsStaleAmount() {
        publisher.publish(new PaymentRequested("P1", "B2", gbp("500.00")));
        publisher.publish(new PaymentAmended("P1", gbp("300.00")));

        // The amendment finds no payment to change, so it is dropped.
        await().atMost(Duration.ofSeconds(30))
                .until(() -> journal.contains(Kind.SKIPPED_UNKNOWN_PAYMENT, "P1"));

        // Retries run out before the payee is verified.
        await().atMost(Duration.ofSeconds(30)).until(() -> deadLetters.payloads().size() == 1);

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
