package com.elgamal.redecide.redecide;

import com.elgamal.redecide.beneficiary.BeneficiaryActivationHandler;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** Re-decide design: activation wakes the payments that were waiting for it. */
@Component
@ConditionalOnProperty(name = "demo.design", havingValue = "redecide")
public class RedecideActivationHandler implements BeneficiaryActivationHandler {

    private final PaymentCommands commands;

    public RedecideActivationHandler(PaymentCommands commands) {
        this.commands = commands;
    }

    @Override
    public void onActivated(String beneficiaryId) {
        commands.onBeneficiaryActivated(beneficiaryId);
    }
}
