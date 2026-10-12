package com.elgamal.payments.throwing;

import com.elgamal.payments.beneficiary.BeneficiaryActivationHandler;
import com.elgamal.payments.beneficiary.BeneficiaryProjection;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

/** Throwing designs: record the activation and let the next retry notice it. */
@Component
@ConditionalOnExpression("'${demo.design}' != 'waiting'")
public class ThrowingActivationHandler implements BeneficiaryActivationHandler {

    private final BeneficiaryProjection beneficiaries;

    public ThrowingActivationHandler(BeneficiaryProjection beneficiaries) {
        this.beneficiaries = beneficiaries;
    }

    @Override
    public void onActivated(String beneficiaryId) {
        beneficiaries.activate(beneficiaryId);
    }
}
