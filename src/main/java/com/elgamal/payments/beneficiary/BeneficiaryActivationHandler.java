package com.elgamal.payments.beneficiary;

/** What a design does when a beneficiary becomes active. */
public interface BeneficiaryActivationHandler {

    void onActivated(String beneficiaryId);
}
