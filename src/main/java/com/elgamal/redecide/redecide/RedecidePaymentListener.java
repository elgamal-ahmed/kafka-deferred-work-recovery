package com.elgamal.redecide.redecide;

import com.elgamal.redecide.events.DomainEvent.BeneficiaryActivated;
import com.elgamal.redecide.events.DomainEvent.PaymentAmended;
import com.elgamal.redecide.events.DomainEvent.PaymentCancelled;
import com.elgamal.redecide.events.DomainEvent.PaymentRequested;
import com.elgamal.redecide.events.EventCodec;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Re-decide design. Nothing here throws for business reasons, so retry infrastructure
 * (Spring's default error handler) only ever sees technical failures.
 */
@Component
@ConditionalOnProperty(name = "demo.design", havingValue = "redecide")
public class RedecidePaymentListener {

    private final EventCodec codec;
    private final PaymentCommands commands;

    public RedecidePaymentListener(EventCodec codec, PaymentCommands commands) {
        this.codec = codec;
        this.commands = commands;
    }

    @KafkaListener(topics = "${demo.topics.payments}", groupId = "${demo.design}-payments")
    public void on(String value) {
        switch (codec.decode(value)) {
            case PaymentRequested e -> commands.accept(e);
            case PaymentAmended e -> commands.amend(e);
            case PaymentCancelled e -> commands.cancel(e);
            case BeneficiaryActivated e -> throw new IllegalArgumentException("Unexpected on payments topic: " + e);
        }
    }
}
