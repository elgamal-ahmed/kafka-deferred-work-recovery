package com.elgamal.payments;

import com.elgamal.payments.beneficiary.BeneficiaryProjection;
import com.elgamal.payments.payment.Payment;
import com.elgamal.payments.payment.PaymentRepository;
import com.elgamal.payments.payment.PaymentStatus;
import com.elgamal.payments.support.EventJournal;
import com.elgamal.payments.support.Outbox;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static org.awaitility.Awaitility.await;

/**
 * Real Kafka and PostgreSQL, started once for the whole test run. Each test class picks a design
 * with {@code demo.design}; each design uses its own topics.
 *
 * <p>The scheduled sweeper is off, so it cannot run in the middle of a controlled race. Tests
 * that need it call {@code PaymentCommands.sweep()} directly.
 */
@SpringBootTest
@TestPropertySource(properties = "demo.sweeper.enabled=false")
@Import(TestHooksConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public abstract class IntegrationTest {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    static {
        POSTGRES.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    protected JdbcTemplate jdbc;

    @Autowired
    protected PaymentRepository payments;

    @Autowired
    protected BeneficiaryProjection beneficiaries;

    @Autowired
    protected Outbox outbox;

    @Autowired
    protected EventJournal journal;

    @Autowired
    protected PausingHooks hooks;

    @Autowired
    private KafkaListenerEndpointRegistry listeners;

    /** Dedicated threads, so a paused transaction can never starve the transaction racing it. */
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @BeforeEach
    void resetState() {
        jdbc.execute("truncate table payment, beneficiary, outbox_event, dead_letter restart identity");
        journal.clear();
        hooks.reset();
    }

    @BeforeEach
    void waitForConsumersToBeAssigned() {
        await().atMost(Duration.ofSeconds(60)).until(() -> listeners.getListenerContainers().stream()
                .allMatch(container -> container.getAssignedPartitions() != null
                        && !container.getAssignedPartitions().isEmpty()));
    }

    @AfterEach
    void stopExecutor() {
        executor.shutdownNow();
    }

    protected <T> CompletableFuture<T> async(Supplier<T> work) {
        return CompletableFuture.supplyAsync(work, executor);
    }

    protected PaymentStatus status(String paymentId) {
        return payments.findById(paymentId).map(Payment::status).orElse(null);
    }

    protected BigDecimal amount(String paymentId) {
        return payments.findById(paymentId).map(Payment::amount).orElse(null);
    }

    protected static BigDecimal gbp(String value) {
        return new BigDecimal(value);
    }
}
