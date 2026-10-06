package com.elgamal.redecide.naive;

import com.elgamal.redecide.IntegrationTest;
import com.elgamal.redecide.beneficiary.BeneficiaryStatus;
import com.elgamal.redecide.events.DomainEvent.BeneficiaryActivated;
import com.elgamal.redecide.events.DomainEvent.PaymentRequested;
import com.elgamal.redecide.events.EventPublisher;
import com.elgamal.redecide.support.EventJournal.Kind;
import com.elgamal.redecide.support.Outbox;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;

import static com.elgamal.redecide.payment.PaymentStatus.SUBMITTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Problem 1: blocking retry keeps partition order by stopping everything behind the failed record. */
@TestPropertySource(properties = "demo.design=blocking")
class BlockingRetryTest extends IntegrationTest {

    @Autowired
    EventPublisher publisher;

    @Test
    void blockingRetryStallsOtherPaymentsOnPartition() {
        publisher.publish(new BeneficiaryActivated("B-OTHER"));
        await().atMost(Duration.ofSeconds(30))
                .until(() -> beneficiaries.read("B-OTHER") == BeneficiaryStatus.ACTIVE);

        // P1's payee is still being verified. P3's payee is active, but P3 is behind P1 on the partition.
        publisher.publish(new PaymentRequested("P1", "B1", gbp("500.00")));
        publisher.publish(new PaymentRequested("P3", "B-OTHER", gbp("75.00")));

        await().atMost(Duration.ofSeconds(30))
                .until(() -> journal.count(Kind.BENEFICIARY_NOT_ACTIVE, "P1") >= 3);

        // P3 could go out right now, yet it does not move while P1 is being retried.
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5))
                .until(() -> status("P3") == null);

        publisher.publish(new BeneficiaryActivated("B1"));

        await().atMost(Duration.ofSeconds(30))
                .until(() -> status("P1") == SUBMITTED && status("P3") == SUBMITTED);
        assertThat(outbox.rows()).extracting(Outbox.Row::aggregateId).containsExactly("P1", "P3");
    }
}
