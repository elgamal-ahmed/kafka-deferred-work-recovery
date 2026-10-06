package com.elgamal.redecide.naive;

public class BeneficiaryNotActiveException extends RuntimeException {

    public BeneficiaryNotActiveException(String beneficiaryId) {
        super("Beneficiary " + beneficiaryId + " is not active yet");
    }
}
