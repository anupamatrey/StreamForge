package com.anupam.streamforge.streams;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.function.Function;

import com.anupam.streamforge.streams.model.Address;
import com.anupam.streamforge.streams.model.AddressCollection;
import com.anupam.streamforge.streams.model.Customer;
import com.anupam.streamforge.streams.model.CustomerDocument;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Component
public class CustomerAggregationTopology {

    public static final String CUSTOMERS_TOPIC = "localserver.public.customer";
    public static final String ADDRESSES_TOPIC = "localserver.public.address";
    public static final String DOCUMENTS_TOPIC = "customer-search-documents";

    private final ObjectMapper mapper;

    public CustomerAggregationTopology(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Autowired
    public void build(StreamsBuilder builder) {
        Serde<Customer> customerSerde = new JsonSerde<>(mapper, Customer.class);
        Serde<Address> addressSerde = new JsonSerde<>(mapper, Address.class);
        Serde<AddressCollection> collectionSerde = new JsonSerde<>(mapper, AddressCollection.class);
        Serde<CustomerDocument> documentSerde = new JsonSerde<>(mapper, CustomerDocument.class);

        // Latest customer state, keyed by customer_id.
        KTable<String, Customer> customers = table(
                builder, CUSTOMERS_TOPIC, "customer_id", this::toCustomer,
                customerSerde, "customers-by-id");

        // Latest address state, keyed by address_id.
        KTable<String, Address> addresses = table(
                builder, ADDRESSES_TOPIC, "address_id", this::toAddress,
                addressSerde, "addresses-by-id");

        // Re-key by customer_id. On an update the old value is subtracted from its
        // previous customer and the new value is added to the current one, so a
        // changed customer_id moves the address between collections.
        KTable<String, AddressCollection> addressesByCustomer = addresses
                .groupBy(
                        (addressId, address) -> KeyValue.pair(String.valueOf(address.customerId()), address),
                        Grouped.with("addresses-by-customer-group", Serdes.String(), addressSerde))
                .aggregate(
                        AddressCollection::new,
                        (customerId, address, aggregate) -> aggregate.with(address),
                        (customerId, oldAddress, aggregate) -> aggregate.without(oldAddress.addressId()),
                        Materialized.<String, AddressCollection, KeyValueStore<Bytes, byte[]>>as("addresses-by-customer")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(collectionSerde));

        // Left join so a customer is published before any address arrives.
        KTable<String, CustomerDocument> documents = customers.leftJoin(
                addressesByCustomer,
                CustomerDocument::from,
                Materialized.<String, CustomerDocument, KeyValueStore<Bytes, byte[]>>as("customer-documents")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(documentSerde));

        documents.toStream().to(DOCUMENTS_TOPIC, Produced.with(Serdes.String(), documentSerde));
    }

    // Reads the Debezium envelope and turns it into a keyed table. Deletes
    // (op "d") become null values, which remove the key from the table.
    private <T> KTable<String, T> table(
            StreamsBuilder builder,
            String topic,
            String idField,
            Function<JsonNode, T> mapperFn,
            Serde<T> serde,
            String storeName) {

        return builder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> value != null)
                .<String, T>flatMap((key, value) -> {
                    JsonNode payload;
                    try {
                        payload = mapper.readTree(value).path("payload");
                    } catch (JacksonException e) {
                        return List.of();
                    }
                    if (payload.isMissingNode() || payload.isNull()) {
                        return List.of();
                    }
                    boolean delete = "d".equals(payload.path("op").asString());
                    JsonNode row = payload.path(delete ? "before" : "after");
                    JsonNode id = row.path(idField);
                    if (id.isMissingNode() || id.isNull()) {
                        return List.of();
                    }
                    return List.of(KeyValue.pair(id.asString(), delete ? null : mapperFn.apply(row)));
                })
                .toTable(Materialized.<String, T, KeyValueStore<Bytes, byte[]>>as(storeName)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(serde));
    }

    private Customer toCustomer(JsonNode row) {
        return new Customer(
                row.path("customer_id").asLong(),
                text(row, "first_name"),
                text(row, "last_name"),
                text(row, "email"),
                microsToIso(row.path("created_at")));
    }

    // Debezium sends Postgres timestamps as epoch microseconds.
    private static String microsToIso(JsonNode node) {
        if (node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (!node.isNumber()) {
            return node.asString();
        }
        return Instant.EPOCH.plus(node.asLong(), ChronoUnit.MICROS).toString();
    }

    private Address toAddress(JsonNode row) {
        return new Address(
                row.path("address_id").asLong(),
                row.path("customer_id").asLong(),
                text(row, "street"),
                text(row, "city"),
                text(row, "state"),
                text(row, "postal_code"),
                text(row, "country"));
    }

    private static String text(JsonNode row, String field) {
        JsonNode node = row.path(field);
        return node.isMissingNode() || node.isNull() ? null : node.asString();
    }
}
