package com.elgamal.redecide.beneficiary;

import com.elgamal.redecide.events.DomainEvent.BeneficiaryActivated;
import com.elgamal.redecide.events.EventCodec;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Separate topic and separate consumer: it keeps running while the payments consumer retries. */
@Component
public class BeneficiaryListener {

    private final EventCodec codec;
    private final BeneficiaryActivationHandler handler;

    public BeneficiaryListener(EventCodec codec, BeneficiaryActivationHandler handler) {
        this.codec = codec;
        this.handler = handler;
    }

    @KafkaListener(topics = "${demo.topics.beneficiaries}", groupId = "${demo.design}-beneficiaries")
    public void on(String value) {
        if (codec.decode(value) instanceof BeneficiaryActivated event) {
            handler.onActivated(event.beneficiaryId());
        }
    }
}
