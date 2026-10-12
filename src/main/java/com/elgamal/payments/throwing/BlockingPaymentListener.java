package com.elgamal.payments.throwing;

import com.elgamal.payments.events.EventCodec;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Blocking design. The DefaultErrorHandler in DemoConfig retries the failed record in place,
 * which keeps partition order by stopping every later record on the partition.
 */
@Component
@ConditionalOnProperty(name = "demo.design", havingValue = "blocking")
public class BlockingPaymentListener {

    private final EventCodec codec;
    private final ThrowingPaymentHandler handler;

    public BlockingPaymentListener(EventCodec codec, ThrowingPaymentHandler handler) {
        this.codec = codec;
        this.handler = handler;
    }

    @KafkaListener(topics = "${demo.topics.payments}", groupId = "${demo.design}-payments")
    public void on(String value) {
        handler.handle(codec.decode(value));
    }
}
