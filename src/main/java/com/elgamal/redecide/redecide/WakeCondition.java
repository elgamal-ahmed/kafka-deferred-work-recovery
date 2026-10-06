package com.elgamal.redecide.redecide;

/**
 * Which event, for which key, can make a waiting payment worth deciding again.
 * The demo has one kind; persisting it is done through the payment's status and beneficiary_id.
 */
public record WakeCondition(String event, String key) {

    public static WakeCondition beneficiaryActivated(String beneficiaryId) {
        return new WakeCondition("BeneficiaryActivated", beneficiaryId);
    }
}
