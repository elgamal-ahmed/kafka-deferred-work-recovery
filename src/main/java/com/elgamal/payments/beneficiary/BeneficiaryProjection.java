package com.elgamal.payments.beneficiary;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * The payment service's local copy of beneficiary status, kept up to date from BeneficiaryActivated.
 * With locking on, deferrals also insert PENDING rows: they exist to be locked, not as data.
 */
@Component
public class BeneficiaryProjection {

    private final JdbcTemplate jdbc;
    private final boolean lockPrecondition;

    public BeneficiaryProjection(JdbcTemplate jdbc,
                                 @Value("${demo.deferral.lock-precondition}") boolean lockPrecondition) {
        this.jdbc = jdbc;
        this.lockPrecondition = lockPrecondition;
    }

    /**
     * Upsert, so activation also waits for any in-flight deferral that inserted or locked this row.
     */
    public void activate(String beneficiaryId) {
        jdbc.update("""
                insert into beneficiary (id, status) values (?, 'ACTIVE')
                on conflict (id) do update set status = 'ACTIVE'
                """, beneficiaryId);
    }

    /** Plain read. A beneficiary we have never heard of is treated as pending. */
    public BeneficiaryStatus read(String beneficiaryId) {
        List<String> rows = jdbc.queryForList(
                "select status from beneficiary where id = ?", String.class, beneficiaryId);
        return rows.isEmpty() ? BeneficiaryStatus.PENDING : BeneficiaryStatus.valueOf(rows.getFirst());
    }

    /**
     * Read used when deciding whether a payment can go ahead or must wait.
     *
     * <p>With locking on, the row is created if missing and then share-locked until the deciding
     * transaction commits. An activation that runs in the meantime blocks on its upsert, so by the
     * time it looks for waiting payments it can see the one this transaction is about to save.
     * Without the insert there would be no row to lock for a beneficiary we have never seen.
     * This only works under READ COMMITTED; see PaymentCommands.onBeneficiaryActivated.
     */
    public BeneficiaryStatus readForDecision(String beneficiaryId) {
        if (!lockPrecondition) {
            return read(beneficiaryId);
        }
        jdbc.update("""
                insert into beneficiary (id, status) values (?, 'PENDING')
                on conflict (id) do nothing
                """, beneficiaryId);
        String status = jdbc.queryForObject(
                "select status from beneficiary where id = ? for share", String.class, beneficiaryId);
        return BeneficiaryStatus.valueOf(status);
    }
}
