package com.elgamal.payments.waiting;

import com.elgamal.payments.beneficiary.BeneficiaryActivationHandler;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** Waiting design: activation wakes the payments that were waiting for it. */
@Component
@ConditionalOnProperty(name = "demo.design", havingValue = "waiting")
public class WaitingActivationHandler implements BeneficiaryActivationHandler {

    private final PaymentCommands commands;

    public WaitingActivationHandler(PaymentCommands commands) {
        this.commands = commands;
    }

    @Override
    public void onActivated(String beneficiaryId) {
        commands.onBeneficiaryActivated(beneficiaryId);
    }
}
