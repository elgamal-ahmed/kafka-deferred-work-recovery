create table if not exists payment (
    id               varchar(64)    primary key,
    beneficiary_id   varchar(64)    not null,
    amount           numeric(19, 2) not null,
    status           varchar(32)    not null,
    version          bigint         not null
);

-- The wake-up query: which payments are waiting for this beneficiary?
create index if not exists payment_waiting_by_beneficiary
    on payment (beneficiary_id)
    where status = 'WAITING_FOR_BENEFICIARY';

-- Local projection of beneficiary status, fed by BeneficiaryActivated.
create table if not exists beneficiary (
    id     varchar(64) primary key,
    status varchar(16) not null
);

-- Written in the same transaction as the payment state change.
-- A relay (polling or CDC) would publish these; it is out of scope for the demo.
create table if not exists outbox_event (
    id           bigserial      primary key,
    aggregate_id varchar(64)    not null,
    event_type   varchar(64)    not null,
    amount       numeric(19, 2),
    created_at   timestamp with time zone not null default now()
);

-- Records that reached the dead-letter topic in the retry-topic design.
create table if not exists dead_letter (
    id         bigserial primary key,
    payload    text      not null,
    created_at timestamp with time zone not null default now()
);
