# Don't Replay It, Re-decide It

[![CI](https://github.com/elgamal-ahmed/kafka-deferred-work-recovery/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/elgamal-ahmed/kafka-deferred-work-recovery/actions/workflows/ci.yml)

Demo code for the talk *Don't Replay It, Re-decide It: Recovering Deferred Work in Kafka and Java*.

A customer adds a new payee and pays them straight away. The payee is still being verified, so the
payment service has to hold the payment until a `BeneficiaryActivated` event arrives on another topic.
Kafka gives no ordering between the two topics.

This repository shows two common ways of handling that with Spring Kafka retries, what goes wrong with
each, and a different model:

> A missing business precondition that can be satisfied later is not a failed execution.
> Store the wait as domain state, wake it on the event that matters, and re-run the domain
> decision against current state instead of replaying the original message.

Every scenario from the talk is a test that runs against real Kafka and PostgreSQL (Testcontainers).
The waiting design's races are reproduced with controlled thread interleaving, so those tests are
deterministic. The retry scenarios depend on the configured backoff, so each of those test classes
sets its own retry settings.

## The scenarios

| # | Scenario | Test |
|---|----------|------|
| 1 | Blocking retry keeps partition order by stopping every later record on the partition | [`BlockingRetryTest`](src/test/java/com/elgamal/payments/throwing/BlockingRetryTest.java) |
| 2a | `@RetryableTopic` keeps the partition moving, so a cancel overtakes the retried request, finds nothing, and the cancelled payment is later submitted | [`NonBlockingRetryTest`](src/test/java/com/elgamal/payments/throwing/NonBlockingRetryTest.java) |
| 2b | The amendment overtakes the retried request and is dropped. Once the retries run out, redriving the dead letter replays the original record: £500 is sent, not the amended £300 | [`DeadLetterRedriveTest`](src/test/java/com/elgamal/payments/throwing/DeadLetterRedriveTest.java) |
| 3a | Wait as domain state: the cancel and the amendment both have a payment to act on, and activation decides again against current state | [`WaitingPaymentTest`](src/test/java/com/elgamal/payments/waiting/WaitingPaymentTest.java) |
| 3b | A cancel commits after a wake-up loaded the payment: the `@Version` check fails, the payment is reloaded, and the decision becomes `Supersede` | [`WaitingPaymentTest`](src/test/java/com/elgamal/payments/waiting/WaitingPaymentTest.java) |
| 3c | An amendment commits after a wake-up loaded the payment: the stale `Execute` and its outbox row roll back, the payment is reloaded, and £300 is sent | [`WaitingPaymentTest`](src/test/java/com/elgamal/payments/waiting/WaitingPaymentTest.java) |
| 4a | The lost wakeup: activation commits between "read: not active" and "save: waiting", and the payment is never woken (until the sweeper runs) | [`LostWakeupTest`](src/test/java/com/elgamal/payments/waiting/LostWakeupTest.java) |
| 4b | The fix, for a beneficiary we have never seen: the deferral inserts the beneficiary row, so activation waits for that insert to commit and then finds the waiting payment | [`LostWakeupFixedTest`](src/test/java/com/elgamal/payments/waiting/LostWakeupFixedTest.java) |
| 4c | The fix, for a beneficiary that is already pending: the deferral share-locks the row, so activation waits for the lock and then finds the waiting payment | [`LostWakeupFixedTest`](src/test/java/com/elgamal/payments/waiting/LostWakeupFixedTest.java) |

Why not retry the amendment or the cancel as well? In the throwing designs a payment is only committed
once it has been submitted, so a retried cancel would still never find an open payment to act on.

The decision itself is a pure function with its own unit test:
[`PaymentDeciderTest`](src/test/java/com/elgamal/payments/waiting/PaymentDeciderTest.java).

## Sequence diagrams

Each diagram follows one of the tests above. Kafka listeners are left out where they only pass the
event on: `WaitingPaymentListener` and `BeneficiaryListener` hand events straight to
`PaymentCommands`, and the throwing listeners hand them to `ThrowingPaymentHandler`.

### The throwing designs

**1. Blocking retry stops the partition** ([`BlockingRetryTest`](src/test/java/com/elgamal/payments/throwing/BlockingRetryTest.java))

```mermaid
sequenceDiagram
    autonumber
    participant PT as payments topic
    participant H as ThrowingPaymentHandler
    participant DB as PostgreSQL
    participant BT as beneficiaries topic

    PT->>H: PaymentRequested P1 for B1, £500
    H->>DB: save P1, read B1
    DB-->>H: B1 not active
    Note over H,DB: throw, rollback, P1 is not saved
    Note over PT,H: DefaultErrorHandler seeks back and retries P1 every 300 ms
    Note over PT: P3 for B-OTHER is next on the partition. Its payee is active, but it waits behind P1
    BT->>DB: BeneficiaryActivated B1, B1 becomes ACTIVE
    PT->>H: retry PaymentRequested P1
    H->>DB: P1 SUBMITTED, outbox row
    PT->>H: PaymentRequested P3, at last
    H->>DB: P3 SUBMITTED, outbox row
```

**2a. With `@RetryableTopic`, the cancel overtakes the request and is lost** ([`NonBlockingRetryTest`](src/test/java/com/elgamal/payments/throwing/NonBlockingRetryTest.java))

```mermaid
sequenceDiagram
    autonumber
    participant PT as payments topic
    participant RT as retry topic
    participant H as ThrowingPaymentHandler
    participant DB as PostgreSQL
    participant BT as beneficiaries topic

    PT->>H: PaymentRequested P2 for B1, £200
    H->>DB: save P2, read B1
    DB-->>H: B1 not active
    Note over H,DB: throw, rollback, P2 is not saved
    H->>RT: Spring Kafka publishes the failed record to the retry topic
    PT->>H: PaymentCancelled P2
    H->>DB: find P2
    DB-->>H: not found
    Note over H: cancel skipped, nothing keeps a record of it
    BT->>DB: BeneficiaryActivated B1, B1 becomes ACTIVE
    RT->>H: retry PaymentRequested P2
    H->>DB: save P2, B1 active, P2 SUBMITTED, outbox row
    Note over H,DB: the cancelled payment goes out
```

**2b. Redriving a dead letter sends the old amount** ([`DeadLetterRedriveTest`](src/test/java/com/elgamal/payments/throwing/DeadLetterRedriveTest.java))

```mermaid
sequenceDiagram
    autonumber
    actor Op as Operator
    participant PT as payments topic
    participant RT as retry and dead-letter topics
    participant H as ThrowingPaymentHandler
    participant DB as PostgreSQL

    PT->>H: PaymentRequested P1 for B2, £500
    Note over H,DB: B2 not active, throw, rollback
    H->>RT: to the retry topic
    PT->>H: PaymentAmended P1 to £300
    H->>DB: find P1
    DB-->>H: not found
    Note over H: amendment skipped
    RT->>H: every retry fails
    H->>RT: to the dead-letter topic
    Note over DB: later, B2 becomes ACTIVE
    Op->>PT: redrive the dead letter, the original record
    PT->>H: PaymentRequested P1, £500
    H->>DB: P1 SUBMITTED, outbox row for £500
    Note over H,DB: the customer asked for £300
```

### The waiting design

**3a. Wait as state, decide again on wake-up** ([`WaitingPaymentTest`](src/test/java/com/elgamal/payments/waiting/WaitingPaymentTest.java))

```mermaid
sequenceDiagram
    autonumber
    participant PT as payments topic
    participant BT as beneficiaries topic
    participant C as PaymentCommands
    participant D as PaymentDecider
    participant DB as PostgreSQL

    PT->>C: 09:00 PaymentRequested P1 for B1, £500
    C->>DB: save P1, read B1 for the decision
    DB-->>C: B1 PENDING
    C->>D: decide(P1, PENDING)
    D-->>C: Wait(B1)
    C->>DB: P1 WAITING_FOR_BENEFICIARY, commit
    PT->>C: 09:01 PaymentRequested P2 for B1, £200
    Note over C,DB: same path, P2 is WAITING_FOR_BENEFICIARY
    PT->>C: 09:04 PaymentAmended P1 to £300
    C->>DB: P1 amount £300
    PT->>C: 09:07 PaymentCancelled P2
    C->>DB: P2 CANCELLED, its wait is over
    BT->>C: 09:10 BeneficiaryActivated B1
    C->>DB: mark B1 ACTIVE, find payments waiting for B1
    DB-->>C: P1 only
    C->>DB: load P1 as it is now
    C->>D: decide(P1, ACTIVE)
    D-->>C: Execute(£300)
    C->>DB: P1 SUBMITTED, outbox row for £300, commit
```

### Corner cases

**3b and 3c. A customer change lands while a wake-up is deciding** ([`WaitingPaymentTest`](src/test/java/com/elgamal/payments/waiting/WaitingPaymentTest.java))

```mermaid
sequenceDiagram
    autonumber
    participant W as Wake-up transaction
    participant DB as PostgreSQL
    participant X as Customer change

    W->>DB: load P5 for £500, WAITING_FOR_BENEFICIARY, version 1
    W->>DB: read B5 for the decision
    DB-->>W: ACTIVE
    alt the customer cancels
        X->>DB: P5 CANCELLED, version 2, commit
    else the customer lowers the amount
        X->>DB: P5 amount £300, version 2, commit
    end
    W->>W: decide on the loaded P5 gives Execute(£500)
    W->>DB: outbox row, then update P5 where version = 1
    DB-->>W: 0 rows, optimistic lock failure
    Note over W,DB: rollback, the outbox row goes with it
    W->>DB: try again: load P5, version 2
    alt it was cancelled
        W->>W: decide gives Supersede, nothing is sent
    else the amount changed
        W->>W: decide gives Execute(£300)
        W->>DB: P5 SUBMITTED, outbox row for £300, commit
    end
```

**4a. The lost wakeup, without the lock** ([`LostWakeupTest`](src/test/java/com/elgamal/payments/waiting/LostWakeupTest.java))

```mermaid
sequenceDiagram
    autonumber
    participant A as Deferral of P7
    participant DB as PostgreSQL
    participant B as Activation of B7
    participant S as Sweeper

    A->>DB: read B7
    DB-->>A: not active
    B->>DB: mark B7 ACTIVE
    B->>DB: find payments waiting for B7
    DB-->>B: none, P7 is not committed yet
    B->>DB: commit
    A->>DB: save P7 WAITING_FOR_BENEFICIARY, commit
    Note over A,B: nothing will wake P7
    S->>DB: find every waiting payment
    DB-->>S: P7
    S->>DB: decide again, Execute, P7 SUBMITTED
```

**4b. The fix, for a beneficiary we have never seen** ([`LostWakeupFixedTest`](src/test/java/com/elgamal/payments/waiting/LostWakeupFixedTest.java))

```mermaid
sequenceDiagram
    autonumber
    participant A as Deferral of P7
    participant DB as PostgreSQL
    participant B as Activation of B7

    A->>DB: insert B7 PENDING on conflict do nothing
    Note over A,DB: row inserted, not committed yet
    A->>DB: select status for share
    DB-->>A: PENDING
    B->>DB: insert B7 ACTIVE on conflict do update
    Note over B,DB: waits until the deferral's insert commits
    A->>DB: save P7 WAITING_FOR_BENEFICIARY, commit
    DB-->>B: conflict, B7 updated to ACTIVE
    B->>DB: find payments waiting for B7, with a new snapshot
    DB-->>B: P7
    B->>DB: decide again, Execute, P7 SUBMITTED
```

**4c. The fix, for a beneficiary that is already pending** ([`LostWakeupFixedTest`](src/test/java/com/elgamal/payments/waiting/LostWakeupFixedTest.java))

```mermaid
sequenceDiagram
    autonumber
    participant A as Deferral of P7
    participant DB as PostgreSQL
    participant B as Activation of B7

    Note over DB: B7 already exists as PENDING, because P6 is waiting for it
    A->>DB: insert B7 PENDING on conflict do nothing
    Note over A,DB: row exists, nothing inserted
    A->>DB: select status for share
    DB-->>A: PENDING, row share-locked until commit
    B->>DB: insert B7 ACTIVE on conflict do update
    Note over B,DB: waits for the share lock
    A->>DB: save P7 WAITING_FOR_BENEFICIARY, commit, lock released
    DB-->>B: B7 updated to ACTIVE
    B->>DB: find payments waiting for B7, with a new snapshot
    DB-->>B: P6 and P7
    B->>DB: decide again on each, Execute
```

Both fixes rely on READ COMMITTED, where the select after the upsert takes a new snapshot. Under
REPEATABLE READ the first case fails with a serialization error, and the second silently misses P7.

## The model

```java
public sealed interface Decision {
    record Execute(BigDecimal amount) implements Decision {}
    record Wait(String beneficiaryId) implements Decision {}
    record Supersede(String reason) implements Decision {}
}
```

- **Don't throw for a missing precondition.** The request and its waiting state commit together.
  The listener only throws for technical failures, and those stay with the retry infrastructure.
- **One decision function**, [`PaymentDecider.decide(payment, beneficiary)`](src/main/java/com/elgamal/payments/waiting/PaymentDecider.java),
  runs on first delivery and on every wake-up. The decision is applied with an exhaustive switch in
  [`PaymentCommands`](src/main/java/com/elgamal/payments/waiting/PaymentCommands.java), so the compiler
  checks that every case is handled.
- **Waiting is explicit state** (`WAITING_FOR_BENEFICIARY`), indexed by `beneficiary_id` so the waking
  event can find it.
- **Wake on the event, sweep as a backstop.** The sweeper decides again on every waiting payment on a
  schedule, in case a wake-up was missed.
- **Saving the wait and waking it must not interleave unsafely.** See
  [`BeneficiaryProjection.readForDecision`](src/main/java/com/elgamal/payments/beneficiary/BeneficiaryProjection.java).
  The fix relies on PostgreSQL's READ COMMITTED isolation, which the application sets explicitly.
- **Execution** changes the payment state and writes an outbox row in one transaction. The payment's
  `@Version` makes sure only one transaction can commit that change. The outbox relay can still
  deliver an event more than once, so whatever consumes `PaymentSubmitted` must ignore duplicates,
  for example by payment id.

## Running the tests

You need Java 21, Maven 3.9+ and a running Docker daemon.

```bash
mvn verify
```

The first run pulls `postgres:16-alpine` and `apache/kafka:3.8.0`.

## Running the application

```bash
docker compose up -d
mvn spring-boot:run -Dspring-boot.run.arguments=--demo.design=waiting
```

`demo.design` selects the consumer design: `blocking`, `retry-topic` or `waiting`. Each design uses its
own topics, for example `waiting.payments` and `waiting.beneficiaries`.

## Project layout

```
src/main/java/com/elgamal/payments
├── events        DomainEvent (sealed), JSON codec, publisher
├── payment       Payment aggregate with @Version
├── beneficiary   local beneficiary projection and its listener
├── throwing      throw and let Kafka retry: blocking and @RetryableTopic listeners
├── waiting       save the wait as state: the decision, wake-up and sweeper
└── support       outbox, dead letters, demo journal, test seam
```

## Deliberately left out

- **The outbox relay.** Rows are written in the same transaction as the state change. Publishing them
  (polling or CDC) is a separate, well-known concern.
- **Kafka transactions.** The throwing designs use a plain database transaction. Spring Kafka does not
  support non-blocking retries with container transactions
  ([docs](https://docs.spring.io/spring-kafka/reference/3.3/retrytopic.html)).
- **Timeouts.** A real waiting payment needs an end, such as a deadline or a beneficiary-rejected
  event. The demo leaves it out to keep the decision to two inputs.
- **Multiple partitions and consumers.** One partition per topic keeps the ordering effects easy to see.
  The problems do not go away with more partitions; they only become harder to observe.
