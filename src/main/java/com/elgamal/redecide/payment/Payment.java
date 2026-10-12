package com.elgamal.redecide.payment;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.math.BigDecimal;

@Entity
@Table(name = "payment")
public class Payment {

    @Id
    private String id;

    private String beneficiaryId;

    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    private PaymentStatus status;

    /** Optimistic lock: two transactions cannot both move the payment on from the same version. */
    @Version
    private Long version;

    protected Payment() {
    }

    public static Payment requested(String id, String beneficiaryId, BigDecimal amount) {
        Payment payment = new Payment();
        payment.id = id;
        payment.beneficiaryId = beneficiaryId;
        payment.amount = amount;
        payment.status = PaymentStatus.REQUESTED;
        return payment;
    }

    public boolean isOpen() {
        return status == PaymentStatus.REQUESTED || status == PaymentStatus.WAITING_FOR_BENEFICIARY;
    }

    public void submit() {
        requireOpen("submit");
        status = PaymentStatus.SUBMITTED;
    }

    public void markWaiting() {
        requireOpen("wait");
        status = PaymentStatus.WAITING_FOR_BENEFICIARY;
    }

    /** Returns false if the payment has already been submitted or cancelled. */
    public boolean amend(BigDecimal newAmount) {
        if (!isOpen()) {
            return false;
        }
        amount = newAmount;
        return true;
    }

    /** Returns false if the payment has already been submitted or cancelled. */
    public boolean cancel() {
        if (!isOpen()) {
            return false;
        }
        status = PaymentStatus.CANCELLED;
        return true;
    }

    private void requireOpen(String action) {
        if (!isOpen()) {
            throw new IllegalStateException("Cannot " + action + " payment " + id + " in status " + status);
        }
    }

    public String id() {
        return id;
    }

    public String beneficiaryId() {
        return beneficiaryId;
    }

    public BigDecimal amount() {
        return amount;
    }

    public PaymentStatus status() {
        return status;
    }
}
