package com.elgamal.redecide.beneficiary;

/** What a design does when a beneficiary becomes active. */
public interface BeneficiaryActivationHandler {

    void onActivated(String beneficiaryId);
}
