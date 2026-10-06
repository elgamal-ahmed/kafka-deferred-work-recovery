package com.elgamal.redecide.redecide;

import com.elgamal.redecide.IntegrationTest;
import com.elgamal.redecide.PausingHooks;
import com.elgamal.redecide.events.DomainEvent.PaymentRequested;
import com.elgamal.redecide.redecide.ProcessingOutcome.Execute;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.elgamal.redecide.payment.PaymentStatus.SUBMITTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Problem 4, with the fix. The deferral inserts the beneficiary row if missing and share-locks it.
 * The activation's upsert on the same row has to wait for the deferral to commit, so when it then
 * looks for waiting payments, P7 is there.
 */
@TestPropertySource(properties = {"demo.design=redecide", "demo.deferral.lock-precondition=true"})
class LostWakeupFixedTest extends IntegrationTest {

    @Autowired
    PaymentCommands commands;

    @Test
    void activationDuringDeferralDoesNotStrandPayment() throws Exception {
        PausingHooks.Pause pause = hooks.pauseOnce("P7");
        CompletableFuture<Optional<ProcessingOutcome>> deferral = async(
                () -> commands.accept(new PaymentRequested("P7", "B7", gbp("70.00"))));
        pause.awaitReached();

        CompletableFuture<Map<String, ProcessingOutcome>> activation =
                async(() -> commands.onBeneficiaryActivated("B7"));

        // The activation is blocked on the row the deferral locked.
        await().atMost(Duration.ofSeconds(10)).until(this::aSessionIsWaitingOnALock);
        assertThat(activation).isNotDone();

        pause.release();
        deferral.get(10, TimeUnit.SECONDS);

        Map<String, ProcessingOutcome> woken = activation.get(10, TimeUnit.SECONDS);
        assertThat(woken.get("P7")).isInstanceOf(Execute.class);
        assertThat(status("P7")).isEqualTo(SUBMITTED);
        assertThat(outbox.rowsFor("P7")).hasSize(1);
    }

    private boolean aSessionIsWaitingOnALock() {
        Integer waiting = jdbc.queryForObject("""
                select count(*) from pg_stat_activity
                where wait_event_type = 'Lock' and datname = current_database()
                """, Integer.class);
        return waiting != null && waiting > 0;
    }
}
