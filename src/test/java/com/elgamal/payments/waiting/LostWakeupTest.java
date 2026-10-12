package com.elgamal.payments.waiting;

import com.elgamal.payments.IntegrationTest;
import com.elgamal.payments.PausingHooks;
import com.elgamal.payments.beneficiary.BeneficiaryStatus;
import com.elgamal.payments.events.DomainEvent.PaymentRequested;
import com.elgamal.payments.waiting.Decision.Execute;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.elgamal.payments.payment.PaymentStatus.SUBMITTED;
import static com.elgamal.payments.payment.PaymentStatus.WAITING_FOR_BENEFICIARY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Problem 4, without the fix: check-then-act across two transactions under READ COMMITTED.
 *
 * <pre>
 *   deferral                          activation
 *   read "B7 not active"
 *                                     mark B7 active
 *                                     look for waiting payments: none
 *                                     commit
 *   save P7 as WAITING, commit
 *   -> nothing will ever wake P7
 * </pre>
 */
@TestPropertySource(properties = {"demo.design=waiting", "demo.deferral.lock-precondition=false"})
class LostWakeupTest extends IntegrationTest {

    @Autowired
    PaymentCommands commands;

    @Test
    void activationDuringDeferralStrandsPaymentWithoutLock() throws Exception {
        PausingHooks.Pause pause = hooks.pauseOnce("P7");
        CompletableFuture<Optional<Decision>> deferral = async(
                () -> commands.accept(new PaymentRequested("P7", "B7", gbp("70.00"))));
        pause.awaitReached();

        Map<String, Decision> woken = commands.onBeneficiaryActivated("B7");
        assertThat(woken).isEmpty();

        pause.release();
        deferral.get(10, TimeUnit.SECONDS);

        assertThat(beneficiaries.read("B7")).isEqualTo(BeneficiaryStatus.ACTIVE);
        assertThat(status("P7")).isEqualTo(WAITING_FOR_BENEFICIARY);
        assertThat(outbox.rows()).isEmpty();

        // The sweeper is the backstop for anything event-driven waking misses.
        assertThat(commands.sweep().get("P7")).isInstanceOf(Execute.class);
        assertThat(status("P7")).isEqualTo(SUBMITTED);
    }
}
