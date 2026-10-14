package com.anupam.streamforge.streams;

import java.util.Properties;

import com.anupam.streamforge.streams.model.CustomerDocument;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class CustomerAggregationTopologyTest {

    private final ObjectMapper mapper = JsonMapper.builder().build();
    private TopologyTestDriver driver;
    private TestInputTopic<String, String> customers;
    private TestInputTopic<String, String> addresses;
    private TestOutputTopic<String, CustomerDocument> output;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        new CustomerAggregationTopology(mapper).build(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        driver = new TopologyTestDriver(builder.build(), props);

        customers = driver.createInputTopic(CustomerAggregationTopology.CUSTOMERS_TOPIC,
                Serdes.String().serializer(), Serdes.String().serializer());
        addresses = driver.createInputTopic(CustomerAggregationTopology.ADDRESSES_TOPIC,
                Serdes.String().serializer(), Serdes.String().serializer());
        output = driver.createOutputTopic(CustomerAggregationTopology.DOCUMENTS_TOPIC,
                Serdes.String().deserializer(), new JsonSerde<>(mapper, CustomerDocument.class).deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private static String event(String op, String before, String after) {
        return "{\"schema\":{},\"payload\":{\"op\":\"" + op + "\",\"before\":" + before + ",\"after\":" + after + "}}";
    }

    private static String customer(int id, String first) {
        return "{\"customer_id\":" + id + ",\"first_name\":\"" + first
                + "\",\"last_name\":\"Doe\",\"email\":\"a@b.c\",\"created_at\":1}";
    }

    private static String address(int id, int customerId, String city) {
        return "{\"address_id\":" + id + ",\"customer_id\":" + customerId + ",\"street\":\"s\",\"city\":\""
                + city + "\",\"state\":\"NY\",\"postal_code\":\"1\",\"country\":\"US\"}";
    }

    private CustomerDocument latest(String key) {
        CustomerDocument last = null;
        for (var record : output.readKeyValuesToList()) {
            if (record.key.equals(key)) {
                last = record.value;
            }
        }
        return last;
    }

    @Test
    void createdAtIsConvertedFromMicrosToIso() {
        customers.pipeInput("k", event("c", "null", customer(1, "Ann")));
        assertEquals("1970-01-01T00:00:00.000001Z", latest("1").createdAt());

        customers.pipeInput("k", event("c", "null",
                customer(1, "Ann").replace("\"created_at\":1", "\"created_at\":1791906846322299")));
        assertEquals("2026-10-13T15:54:06.322299Z", latest("1").createdAt());
    }

    @Test
    void customerPublishedBeforeAnyAddress() {
        customers.pipeInput("k", event("c", "null", customer(1, "Ann")));
        CustomerDocument doc = latest("1");
        assertEquals("Ann", doc.firstName());
        assertEquals(0, doc.addresses().size());
    }

    @Test
    void addressesAreJoinedAndUpdatedInPlace() {
        customers.pipeInput("k", event("c", "null", customer(1, "Ann")));
        addresses.pipeInput("a", event("c", "null", address(10, 1, "Boston")));
        addresses.pipeInput("a", event("c", "null", address(11, 1, "Austin")));
        addresses.pipeInput("a", event("u", address(10, 1, "Boston"), address(10, 1, "Chicago")));

        CustomerDocument doc = latest("1");
        assertEquals(2, doc.addresses().size());
        assertEquals("Chicago", doc.addresses().get(0).city());
        assertEquals("Austin", doc.addresses().get(1).city());
    }

    @Test
    void addressMovesBetweenCustomers() {
        customers.pipeInput("k1", event("c", "null", customer(1, "Ann")));
        customers.pipeInput("k2", event("c", "null", customer(2, "Bob")));
        addresses.pipeInput("a", event("c", "null", address(10, 1, "Boston")));
        addresses.pipeInput("a", event("u", address(10, 1, "Boston"), address(10, 2, "Boston")));

        CustomerDocument ann = null;
        CustomerDocument bob = null;
        for (var r : output.readKeyValuesToList()) {
            if (r.key.equals("1")) ann = r.value;
            if (r.key.equals("2")) bob = r.value;
        }
        assertEquals(0, ann.addresses().size());
        assertEquals(1, bob.addresses().size());
    }

    @Test
    void deletedAddressIsRemovedAndDeletedCustomerIsTombstoned() {
        customers.pipeInput("k", event("c", "null", customer(1, "Ann")));
        addresses.pipeInput("a", event("c", "null", address(10, 1, "Boston")));
        addresses.pipeInput("a", event("d", address(10, 1, "Boston"), "null"));
        addresses.pipeInput("a", (String) null);
        assertEquals(0, latest("1").addresses().size());

        customers.pipeInput("k", event("d", customer(1, "Ann"), "null"));
        var records = output.readKeyValuesToList();
        assertNull(records.get(records.size() - 1).value);
    }
}
