package com.elgamal.payments;

import com.elgamal.payments.waiting.PaymentDecider;
import com.elgamal.payments.support.ProcessingHooks;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class DemoConfig {

    /** One partition each, so "later records on the same partition" is easy to show. */
    @Bean
    NewTopic paymentsTopic(@Value("${demo.topics.payments}") String name) {
        return TopicBuilder.name(name).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic beneficiariesTopic(@Value("${demo.topics.beneficiaries}") String name) {
        return TopicBuilder.name(name).partitions(1).replicas(1).build();
    }

    @Bean
    PaymentDecider paymentDecider() {
        return new PaymentDecider();
    }

    @Bean
    ProcessingHooks processingHooks() {
        return ProcessingHooks.NONE;
    }

    /**
     * Blocking design only: retry the failed record in place. Later records on the partition
     * do not progress until it succeeds or the attempts run out.
     */
    @Bean
    @ConditionalOnProperty(name = "demo.design", havingValue = "blocking")
    DefaultErrorHandler blockingRetryErrorHandler(@Value("${demo.blocking.backoff-ms}") long backoffMs) {
        return new DefaultErrorHandler(new FixedBackOff(backoffMs, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
