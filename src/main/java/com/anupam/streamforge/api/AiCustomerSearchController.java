package com.anupam.streamforge.api;

import com.anupam.streamforge.ai.service.AiCustomerSearchService;
import com.anupam.streamforge.config.AiGatewayProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/customers")
public class AiCustomerSearchController {

    private static final Logger log = LoggerFactory.getLogger(AiCustomerSearchController.class);

    private final AiCustomerSearchService aiCustomerSearchService;
    private final AiGatewayProperties properties;

    public AiCustomerSearchController(AiCustomerSearchService aiCustomerSearchService, AiGatewayProperties properties) {
        this.aiCustomerSearchService = aiCustomerSearchService;
        this.properties = properties;
    }

    @PostMapping(value = "/ai-search", produces = MediaType.APPLICATION_JSON_VALUE)
    public AiCustomerSearchService.AiSearchResponse search(@RequestBody AiSearchRequest request) {
        int page = request.page() == null ? 0 : request.page();
        int size = request.size() == null ? 10 : request.size();
        log.info("AI search request received (gateway={}, promptLength={}, page={}, size={})",
                properties.displayName(),
                request.prompt() == null ? 0 : request.prompt().length(), page, size);
        long start = System.nanoTime();
        AiCustomerSearchService.AiSearchResponse response = aiCustomerSearchService.search(request.prompt(), page, size);
        log.info("AI search request finished: gateway={}, found={}, total={}, totalTimeMs={}",
                properties.displayName(), response.found(), response.results().total(), (System.nanoTime() - start) / 1_000_000);
        return response;
    }

    public record AiSearchRequest(String prompt, Integer page, Integer size) {}
}
