package com.anupam.streamforge.api;

import java.util.Map;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetResponse;
import com.anupam.streamforge.service.CustomerSearchService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/customers")
public class CustomerSearchController {

    private final ElasticsearchClient client;
    private final CustomerSearchService customerSearchService;

    public CustomerSearchController(ElasticsearchClient client, CustomerSearchService customerSearchService) {
        this.client = client;
        this.customerSearchService = customerSearchService;
    }

    @GetMapping("/search")
    public CustomerSearchService.SearchResult search(
            @RequestParam(required = false, defaultValue = "") String q,
            @RequestParam(required = false) String lastName,
            @RequestParam(required = false) String city,
            @RequestParam(required = false) String state,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        return customerSearchService.search(q, lastName, city, state, page, size);
    }

    @GetMapping("/{id}")
    @SuppressWarnings("unchecked")
    public ResponseEntity<Map<String, Object>> get(@PathVariable long id) throws java.io.IOException {
        GetResponse<Map> response = client.get(
                g -> g.index(CustomerSearchService.INDEX).id(String.valueOf(id)), Map.class);
        if (!response.found()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "Customer not found"));
        }
        return ResponseEntity.ok((Map<String, Object>) response.source());
    }
}
