package com.elgamal.payments.events;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.math.BigDecimal;

/**
 * Events in the demo. Payment events are keyed by paymentId on the payments topic.
 * BeneficiaryActivated is keyed by beneficiaryId on a separate topic, so Kafka gives
 * no ordering between it and the payment events.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DomainEvent.PaymentRequested.class, name = "PaymentRequested"),
        @JsonSubTypes.Type(value = DomainEvent.PaymentAmended.class, name = "PaymentAmended"),
        @JsonSubTypes.Type(value = DomainEvent.PaymentCancelled.class, name = "PaymentCancelled"),
        @JsonSubTypes.Type(value = DomainEvent.BeneficiaryActivated.class, name = "BeneficiaryActivated")
})
public sealed interface DomainEvent {

    record PaymentRequested(String paymentId, String beneficiaryId, BigDecimal amount) implements DomainEvent {
    }

    record PaymentAmended(String paymentId, BigDecimal amount) implements DomainEvent {
    }

    record PaymentCancelled(String paymentId) implements DomainEvent {
    }

    record BeneficiaryActivated(String beneficiaryId) implements DomainEvent {
    }
}
