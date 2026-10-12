package com.anupam.streamforge.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Component
public class CDCConsumer {

    private final ObjectMapper objectMapper;

    public CDCConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "localserver.public.customers",
            groupId = "streamforge-aggregator"
    )
    public void consume(@Payload(required = false) String message) {
        if (message == null) {
            return;
        }
        JsonNode payload;
        try {
            payload = objectMapper.readTree(message).path("payload");
        } catch (JacksonException e) {
            System.err.println("Skipping non-JSON message: " + message);
            return;
        }
        if (payload.isMissingNode() || payload.isNull()) {
            return;
        }

        String op = payload.path("op").asString();
        JsonNode before = payload.path("before");
        JsonNode after = payload.path("after");

        System.out.println("Received CDC payload: op=" + op);
        System.out.println("before=" + before);
        System.out.println("after=" + after);
    }
}
