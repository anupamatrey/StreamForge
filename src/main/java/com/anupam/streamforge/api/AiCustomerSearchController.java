package com.anupam.streamforge.api;

import com.anupam.streamforge.ai.service.AiCustomerSearchService;
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

    public AiCustomerSearchController(AiCustomerSearchService aiCustomerSearchService) {
        this.aiCustomerSearchService = aiCustomerSearchService;
    }

    @PostMapping(value = "/ai-search", produces = MediaType.APPLICATION_JSON_VALUE)
    public AiCustomerSearchService.AiSearchResponse search(@RequestBody AiSearchRequest request) {
        int page = request.page() == null ? 0 : request.page();
        int size = request.size() == null ? 10 : request.size();
        log.info("AI search request received (promptLength={}, page={}, size={})",
                request.prompt() == null ? 0 : request.prompt().length(), page, size);
        long start = System.nanoTime();
        AiCustomerSearchService.AiSearchResponse response = aiCustomerSearchService.search(request.prompt(), page, size);
        log.info("AI search request finished: found={}, total={}, totalTimeMs={}",
                response.found(), response.results().total(), (System.nanoTime() - start) / 1_000_000);
        return response;
    }

    public record AiSearchRequest(String prompt, Integer page, Integer size) {}
}
