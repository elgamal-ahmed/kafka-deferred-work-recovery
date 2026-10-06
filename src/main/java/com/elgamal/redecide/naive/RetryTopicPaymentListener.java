package com.elgamal.redecide.naive;

import com.elgamal.redecide.events.EventCodec;
import com.elgamal.redecide.support.DeadLetters;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * Non-blocking design. A failed record is moved to a retry topic and the main topic keeps going.
 * Throughput recovers, but a later record for the same payment can now be processed first.
 * Spring's documentation is explicit that this gives up Kafka's ordering guarantees.
 */
@Component
@ConditionalOnProperty(name = "demo.design", havingValue = "retry-topic")
public class RetryTopicPaymentListener {

    private final EventCodec codec;
    private final NaivePaymentHandler handler;
    private final DeadLetters deadLetters;

    public RetryTopicPaymentListener(EventCodec codec, NaivePaymentHandler handler, DeadLetters deadLetters) {
        this.codec = codec;
        this.handler = handler;
        this.deadLetters = deadLetters;
    }

    @RetryableTopic(
            attempts = "${demo.retry-topic.attempts}",
            backoff = @Backoff(delayExpression = "${demo.retry-topic.delay-ms}"),
            numPartitions = "1",
            replicationFactor = "1")
    @KafkaListener(topics = "${demo.topics.payments}", groupId = "${demo.design}-payments")
    public void on(String value) {
        handler.handle(codec.decode(value));
    }

    @DltHandler
    public void onDeadLetter(String value) {
        deadLetters.save(value);
    }
}
