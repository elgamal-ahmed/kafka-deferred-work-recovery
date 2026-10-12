package com.elgamal.payments.events;

import com.elgamal.payments.events.DomainEvent.BeneficiaryActivated;
import com.elgamal.payments.events.DomainEvent.PaymentAmended;
import com.elgamal.payments.events.DomainEvent.PaymentCancelled;
import com.elgamal.payments.events.DomainEvent.PaymentRequested;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class EventPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final EventCodec codec;
    private final String paymentsTopic;
    private final String beneficiariesTopic;

    public EventPublisher(KafkaTemplate<String, String> kafka,
                          EventCodec codec,
                          @Value("${demo.topics.payments}") String paymentsTopic,
                          @Value("${demo.topics.beneficiaries}") String beneficiariesTopic) {
        this.kafka = kafka;
        this.codec = codec;
        this.paymentsTopic = paymentsTopic;
        this.beneficiariesTopic = beneficiariesTopic;
    }

    public void publish(DomainEvent event) {
        String topic = event instanceof BeneficiaryActivated ? beneficiariesTopic : paymentsTopic;
        String key = switch (event) {
            case PaymentRequested e -> e.paymentId();
            case PaymentAmended e -> e.paymentId();
            case PaymentCancelled e -> e.paymentId();
            case BeneficiaryActivated e -> e.beneficiaryId();
        };
        try {
            kafka.send(topic, key, codec.encode(event)).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Could not publish " + event, e);
        }
    }
}
