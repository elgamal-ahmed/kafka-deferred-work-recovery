package com.elgamal.redecide.support;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/** Stores what reached the dead-letter topic, so an operator (or a test) can redrive it. */
@Component
public class DeadLetters {

    private final JdbcTemplate jdbc;

    public DeadLetters(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void save(String payload) {
        jdbc.update("insert into dead_letter (payload) values (?)", payload);
    }

    public List<String> payloads() {
        return jdbc.queryForList("select payload from dead_letter order by id", String.class);
    }
}
