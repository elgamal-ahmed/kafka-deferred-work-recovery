package com.elgamal.payments.throwing;

public class BeneficiaryNotActiveException extends RuntimeException {

    public BeneficiaryNotActiveException(String beneficiaryId) {
        super("Beneficiary " + beneficiaryId + " is not active yet");
    }
}
