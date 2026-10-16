package com.anupam.streamforge.exception;

public class AiGatewayException extends RuntimeException {

    public AiGatewayException(String message) {
        super(message);
    }

    public AiGatewayException(String message, Throwable cause) {
        super(message, cause);
    }
}
