package com.elgamal.payments.waiting;

import com.elgamal.payments.IntegrationTest;
import com.elgamal.payments.PausingHooks;
import com.elgamal.payments.events.DomainEvent.BeneficiaryActivated;
import com.elgamal.payments.events.DomainEvent.PaymentAmended;
import com.elgamal.payments.events.DomainEvent.PaymentCancelled;
import com.elgamal.payments.events.DomainEvent.PaymentRequested;
import com.elgamal.payments.events.EventPublisher;
import com.elgamal.payments.waiting.Decision.Execute;
import com.elgamal.payments.waiting.Decision.Supersede;
import com.elgamal.payments.support.EventJournal.Kind;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.elgamal.payments.payment.PaymentStatus.CANCELLED;
import static com.elgamal.payments.payment.PaymentStatus.SUBMITTED;
import static com.elgamal.payments.payment.PaymentStatus.WAITING_FOR_BENEFICIARY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Problem 3: on wake-up, run the decision again against current state instead of replaying. */
@TestPropertySource(properties = "demo.design=waiting")
class WaitingPaymentTest extends IntegrationTest {

    @Autowired
    EventPublisher publisher;

    @Autowired
    PaymentCommands commands;

    @Test
    void wakeUpDecidesAgainOnCurrentState() {
        // 09:00 and 09:01: both payments are saved as WAITING, nothing throws.
        publisher.publish(new PaymentRequested("P1", "B1", gbp("500.00")));
        publisher.publish(new PaymentRequested("P2", "B1", gbp("200.00")));
        await().atMost(Duration.ofSeconds(30)).until(() ->
                status("P1") == WAITING_FOR_BENEFICIARY && status("P2") == WAITING_FOR_BENEFICIARY);

        // 09:04 and 09:07: the amendment and the cancel each have a payment to act on.
        publisher.publish(new PaymentAmended("P1", gbp("300.00")));
        publisher.publish(new PaymentCancelled("P2"));
        await().atMost(Duration.ofSeconds(30)).until(() ->
                status("P2") == CANCELLED && amount("P1").compareTo(gbp("300.00")) == 0);

        // 09:10: activation wakes only P1. P2's cancel already ended its wait.
        publisher.publish(new BeneficiaryActivated("B1"));

        await().atMost(Duration.ofSeconds(30)).until(() -> status("P1") == SUBMITTED);
        assertThat(status("P2")).isEqualTo(CANCELLED);
        assertThat(outbox.rows()).hasSize(1);
        assertThat(outbox.rowsFor("P1").getFirst().amount()).isEqualByComparingTo("300.00");
    }

    @Test
    void cancelAfterLoadFailsVersionCheckAndSupersedes() throws Exception {
        commands.accept(new PaymentRequested("P5", "B5", gbp("50.00")));
        assertThat(status("P5")).isEqualTo(WAITING_FOR_BENEFICIARY);

        // The wake-up loads P5 while it is still WAITING, then pauses before writing its decision.
        PausingHooks.Pause pause = hooks.pauseOnce("P5");
        CompletableFuture<Map<String, Decision>> wakeUp =
                async(() -> commands.onBeneficiaryActivated("B5"));
        pause.awaitReached();

        // In that gap, the customer's cancel commits.
        commands.cancel(new PaymentCancelled("P5"));
        pause.release();

        // The stale Execute fails the @Version check, P5 is reloaded, and the decision is Supersede.
        Map<String, Decision> outcomes = wakeUp.get(10, TimeUnit.SECONDS);
        assertThat(outcomes.get("P5")).isInstanceOf(Supersede.class);
        assertThat(journal.contains(Kind.OPTIMISTIC_CONFLICT, "P5")).isTrue();
        assertThat(status("P5")).isEqualTo(CANCELLED);
        assertThat(outbox.rows()).isEmpty();
    }

    @Test
    void amendAfterLoadFailsVersionCheckAndSendsNewAmount() throws Exception {
        commands.accept(new PaymentRequested("P6", "B6", gbp("500.00")));
        assertThat(status("P6")).isEqualTo(WAITING_FOR_BENEFICIARY);

        // The wake-up loads P6 at £500, then pauses before writing its decision.
        PausingHooks.Pause pause = hooks.pauseOnce("P6");
        CompletableFuture<Map<String, Decision>> wakeUp =
                async(() -> commands.onBeneficiaryActivated("B6"));
        pause.awaitReached();

        // In that gap, the customer lowers the amount to £300.
        commands.amend(new PaymentAmended("P6", gbp("300.00")));
        pause.release();

        // The stale Execute(£500) fails the @Version check and its outbox row rolls back with it.
        // P6 is reloaded and the decision is made again, for £300.
        Map<String, Decision> outcomes = wakeUp.get(10, TimeUnit.SECONDS);
        assertThat(outcomes.get("P6")).isInstanceOf(Execute.class);
        assertThat(journal.count(Kind.OPTIMISTIC_CONFLICT, "P6")).isEqualTo(1);
        assertThat(status("P6")).isEqualTo(SUBMITTED);
        assertThat(outbox.rows()).hasSize(1);
        assertThat(outbox.rowsFor("P6").getFirst().amount()).isEqualByComparingTo("300.00");
    }
}
