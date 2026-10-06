package com.elgamal.redecide.support;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

/**
 * Rows are inserted in the same transaction as the payment state change, so a submitted payment
 * and its PaymentSubmitted event are committed or rolled back together.
 */
@Component
public class Outbox {

    public record Row(long id, String aggregateId, String eventType, BigDecimal amount) {
    }

    private final JdbcTemplate jdbc;

    public Outbox(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void paymentSubmitted(String paymentId, BigDecimal amount) {
        jdbc.update("insert into outbox_event (aggregate_id, event_type, amount) values (?, 'PaymentSubmitted', ?)",
                paymentId, amount);
    }

    public List<Row> rows() {
        return jdbc.query("select id, aggregate_id, event_type, amount from outbox_event order by id",
                (rs, n) -> new Row(rs.getLong("id"), rs.getString("aggregate_id"),
                        rs.getString("event_type"), rs.getBigDecimal("amount")));
    }

    public List<Row> rowsFor(String paymentId) {
        return rows().stream().filter(row -> row.aggregateId().equals(paymentId)).toList();
    }
}
