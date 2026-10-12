package com.elgamal.payments.waiting;

import com.elgamal.payments.IntegrationTest;
import com.elgamal.payments.PausingHooks;
import com.elgamal.payments.events.DomainEvent.PaymentRequested;
import com.elgamal.payments.waiting.Decision.Execute;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.elgamal.payments.payment.PaymentStatus.SUBMITTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Problem 4, with the fix. The deferral inserts the beneficiary row if missing and share-locks it.
 * The activation's upsert on the same row has to wait for the deferral to commit, so when it then
 * looks for waiting payments, P7 is there.
 *
 * <p>Each half of the fix has its own test. For a beneficiary we have never seen, the activation
 * waits on the deferral's uncommitted insert. For one that is already PENDING, it waits on the
 * share lock.
 */
@TestPropertySource(properties = {"demo.design=waiting", "demo.deferral.lock-precondition=true"})
class LostWakeupFixedTest extends IntegrationTest {

    @Autowired
    PaymentCommands commands;

    @Autowired
    TransactionTemplate tx;

    @Test
    void activationWaitsForDeferralOfUnknownBeneficiary() throws Exception {
        Map<String, Decision> woken = activateB7WhileP7IsBeingDeferred();

        assertThat(woken.get("P7")).isInstanceOf(Execute.class);
        assertThat(status("P7")).isEqualTo(SUBMITTED);
        assertThat(outbox.rowsFor("P7")).hasSize(1);
    }

    @Test
    void activationWaitsForDeferralOfPendingBeneficiary() throws Exception {
        // B7 is already known: an earlier payment is waiting for it.
        commands.accept(new PaymentRequested("P6", "B7", gbp("60.00")));

        Map<String, Decision> woken = activateB7WhileP7IsBeingDeferred();

        assertThat(woken.get("P6")).isInstanceOf(Execute.class);
        assertThat(woken.get("P7")).isInstanceOf(Execute.class);
        assertThat(status("P7")).isEqualTo(SUBMITTED);
        assertThat(outbox.rowsFor("P7")).hasSize(1);
    }

    /** The fix depends on each statement taking a new snapshot, which is READ COMMITTED. */
    @Test
    void transactionsRunUnderReadCommitted() {
        String isolation = tx.execute(status ->
                jdbc.queryForObject("show transaction_isolation", String.class));
        assertThat(isolation).isEqualTo("read committed");
    }

    private Map<String, Decision> activateB7WhileP7IsBeingDeferred() throws Exception {
        PausingHooks.Pause pause = hooks.pauseOnce("P7");
        CompletableFuture<Optional<Decision>> deferral = async(
                () -> commands.accept(new PaymentRequested("P7", "B7", gbp("70.00"))));
        pause.awaitReached();

        CompletableFuture<Map<String, Decision>> activation =
                async(() -> commands.onBeneficiaryActivated("B7"));

        // The activation's upsert is blocked by the paused deferral.
        await().atMost(Duration.ofSeconds(10)).until(this::activationIsBlocked);
        assertThat(activation).isNotDone();

        pause.release();
        deferral.get(10, TimeUnit.SECONDS);
        return activation.get(10, TimeUnit.SECONDS);
    }

    private boolean activationIsBlocked() {
        Integer blocked = jdbc.queryForObject("""
                select count(*) from pg_stat_activity
                where query like '%on conflict (id) do update%'
                  and cardinality(pg_blocking_pids(pid)) > 0
                """, Integer.class);
        return blocked != null && blocked > 0;
    }
}
