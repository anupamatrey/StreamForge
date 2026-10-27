package com.anupam.streamforge.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.ConstructorBinding;

@ConfigurationProperties(prefix = "streamforge.ai.gateway")
public record AiGatewayProperties(String provider, String baseUrl, String apiKey, String model,
                                  String upstreamProvider, String upstreamApiKey, String configId) {

    @ConstructorBinding
    public AiGatewayProperties(String provider, String baseUrl, String apiKey, String model,
                               String upstreamProvider, String upstreamApiKey, String configId) {
        this.provider = provider;
        this.baseUrl = baseUrl;
        this.apiKey = apiKey;
        this.model = model;
        this.upstreamProvider = upstreamProvider;
        this.upstreamApiKey = upstreamApiKey;
        this.configId = configId;
    }

    public AiGatewayProperties(String provider, String baseUrl, String apiKey, String model) {
        this(provider, baseUrl, apiKey, model, null, null, null);
    }

    public boolean isPortkey() {
        return "portkey".equalsIgnoreCase(provider);
    }

    public String displayName() {
        return provider == null || provider.isBlank() ? "AI gateway" : provider;
    }
}
