package com.anupam.streamforge.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "streamforge.ai.gateway")
public record AiGatewayProperties(String baseUrl, String apiKey, String model) {
}
