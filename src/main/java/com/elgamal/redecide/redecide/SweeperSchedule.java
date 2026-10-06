package com.elgamal.redecide.redecide;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnExpression("${demo.sweeper.enabled:false} and '${demo.design}' == 'redecide'")
public class SweeperSchedule {

    private final PaymentCommands commands;

    public SweeperSchedule(PaymentCommands commands) {
        this.commands = commands;
    }

    @Scheduled(fixedDelayString = "${demo.sweeper.interval-ms}")
    public void sweep() {
        commands.sweep();
    }
}
