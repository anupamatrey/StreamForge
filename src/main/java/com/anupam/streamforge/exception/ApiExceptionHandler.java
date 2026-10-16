package com.anupam.streamforge.exception;

import java.io.IOException;
import java.util.Map;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler({IllegalArgumentException.class, MethodArgumentTypeMismatchException.class})
    public ResponseEntity<Map<String, String>> badRequest(Exception e) {
        log.warn("Bad request: {}", e.getMessage());
        String message = e instanceof IllegalArgumentException && e.getMessage() != null
                ? e.getMessage() : "Invalid request parameters";
        return ResponseEntity.badRequest().body(Map.of("error", message));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, String>> unreadableBody(HttpMessageNotReadableException e) {
        log.warn("Unreadable request body");
        return ResponseEntity.badRequest().body(Map.of("error", "Request body must be valid JSON, e.g. {\"prompt\": \"...\"}"));
    }

    @ExceptionHandler({IOException.class, ElasticsearchException.class})
    public ResponseEntity<Map<String, String>> upstream(Exception e) {
        log.error("Backend request failed", e);
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(Map.of("error", "Search backend unavailable"));
    }

    @ExceptionHandler(AiGatewayException.class)
    public ResponseEntity<Map<String, String>> aiGateway(AiGatewayException e) {
        log.error("LiteLLM request failed", e);
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                .body(Map.of("error", "AI search is currently unavailable"));
    }

    @ExceptionHandler(SearchBackendException.class)
    public ResponseEntity<Map<String, String>> searchBackend(SearchBackendException e) {
        log.error("Elasticsearch search failed", e);
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(Map.of("error", "Search backend unavailable"));
    }
}
