package com.anupam.streamforge.consumer;

import java.io.IOException;
import java.util.Map;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.anupam.streamforge.streams.CustomerAggregationTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

@Component
public class CustomerDocumentIndexer {

    static final String INDEX = "customer-search";

    private static final Logger log = LoggerFactory.getLogger(CustomerDocumentIndexer.class);

    private final ElasticsearchClient client;
    private final ObjectMapper objectMapper;

    public CustomerDocumentIndexer(ElasticsearchClient client, ObjectMapper objectMapper) {
        this.client = client;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = CustomerAggregationTopology.DOCUMENTS_TOPIC, groupId = "streamforge-aggregator")
    public void index(@Header(KafkaHeaders.RECEIVED_KEY) String customerId,
                      @Payload(required = false) String document) throws IOException {
        if (document == null) {
            log.info("Deleting customer {} from index {}", customerId, INDEX);
            client.delete(d -> d.index(INDEX).id(customerId));
            return;
        }
        Map<String, Object> doc = objectMapper.readValue(document, new TypeReference<>() {});
        log.info("Indexing customer {} into index {}: {}", customerId, INDEX, document);
        client.index(i -> i.index(INDEX).id(customerId).document(doc));
    }
}
