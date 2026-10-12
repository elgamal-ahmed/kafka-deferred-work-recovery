package com.elgamal.payments.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class EventCodec {

    private final ObjectMapper mapper;

    public EventCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String encode(DomainEvent event) {
        try {
            return mapper.writerFor(DomainEvent.class).writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not encode " + event, e);
        }
    }

    public DomainEvent decode(String json) {
        try {
            return mapper.readValue(json, DomainEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not decode " + json, e);
        }
    }
}
