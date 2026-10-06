package com.elgamal.redecide.naive;

import com.elgamal.redecide.beneficiary.BeneficiaryActivationHandler;
import com.elgamal.redecide.beneficiary.BeneficiaryProjection;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

/** Naive designs: record the activation and let the next retry notice it. */
@Component
@ConditionalOnExpression("'${demo.design}' != 'redecide'")
public class NaiveActivationHandler implements BeneficiaryActivationHandler {

    private final BeneficiaryProjection beneficiaries;

    public NaiveActivationHandler(BeneficiaryProjection beneficiaries) {
        this.beneficiaries = beneficiaries;
    }

    @Override
    public void onActivated(String beneficiaryId) {
        beneficiaries.activate(beneficiaryId);
    }
}
