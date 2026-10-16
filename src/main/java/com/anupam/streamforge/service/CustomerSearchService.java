package com.anupam.streamforge.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.TextQueryType;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.anupam.streamforge.exception.SearchBackendException;
import org.springframework.stereotype.Service;

@Service
public class CustomerSearchService {

    public static final String INDEX = "customer-search";
    static final int MAX_PAGE_SIZE = 50;
    static final int MAX_QUERY_LENGTH = 100;
    private static final List<String> TEXT_FIELDS = List.of(
            "firstName", "lastName", "email",
            "addresses.street", "addresses.city", "addresses.state",
            "addresses.postalCode", "addresses.country");

    private final ElasticsearchClient client;

    public CustomerSearchService(ElasticsearchClient client) {
        this.client = client;
    }

    public SearchResult search(String q, String lastName, String city, String state, int page, int size) {
        String text = q == null ? "" : q.strip();
        if (text.length() > MAX_QUERY_LENGTH) {
            throw new IllegalArgumentException("q must be at most " + MAX_QUERY_LENGTH + " characters");
        }
        int safePage = Math.min(Math.max(page, 0), 1000);
        int safeSize = Math.min(Math.max(size, 1), MAX_PAGE_SIZE);

        Query query = Query.of(b -> b.bool(bool -> {
            if (text.isEmpty()) {
                bool.must(m -> m.matchAll(a -> a));
            } else {
                bool.must(m -> m.multiMatch(mm -> mm
                        .query(text)
                        .fields(TEXT_FIELDS)
                        .type(TextQueryType.BestFields)
                        .fuzziness("AUTO")));
            }
            if (lastName != null && !lastName.isBlank()) {
                bool.filter(f -> f.match(m -> m.field("lastName").query(FieldValue.of(lastName.strip()))));
            }
            if (city != null && !city.isBlank()) {
                bool.filter(f -> f.match(m -> m.field("addresses.city").query(FieldValue.of(city.strip()))));
            }
            if (state != null && !state.isBlank()) {
                bool.filter(f -> f.match(m -> m.field("addresses.state").query(FieldValue.of(state.strip()))));
            }
            return bool;
        }));

        try {
            SearchResponse<Map> response = client.search(s -> s
                            .index(INDEX)
                            .ignoreUnavailable(true)
                            .query(query)
                            .from(safePage * safeSize)
                            .size(safeSize),
                    Map.class);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> items = response.hits().hits().stream()
                    .map(Hit::source)
                    .map(source -> (Map<String, Object>) source)
                    .toList();
            long total = response.hits().total() == null ? items.size() : response.hits().total().value();
            return new SearchResult(total, safePage, safeSize, items);
        } catch (IOException e) {
            throw new SearchBackendException("Elasticsearch search failed", e);
        }
    }

    public record SearchResult(long total, int page, int size, List<Map<String, Object>> items) {}
}
