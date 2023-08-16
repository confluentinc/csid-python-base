package io.confluent.pytools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;


public class TestEnd2End extends KafkaConnectBase {
    @Test
    /*
     * Test topology: source connector --> full python message --> topic --> consumer 1 checks msg (Long + String)
     */
    @SneakyThrows
    public void sourceConnectorToConsumerLongString() {
        String topicName = "test-topic-" + UUID.randomUUID();

        ObjectMapper connectorConfigMapper = new ObjectMapper();

        Map<String, Object> connectorMap = new HashMap<>();
        connectorMap.put("name", "pytools-test-1");
        connectorMap.put("config", new HashMap<>());

        Map<String, Object> configMap = (Map<String, Object>) connectorMap.get("config");
        configMap.put("connector.class", "io.confluent.pytools.PySourceConnector");
        configMap.put("kafka.topic", topicName);
        configMap.put("key.converter", "org.apache.kafka.connect.converters.LongConverter");
        configMap.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configMap.put("value.converter.schemas.enable", "false");

        configMap.put("scripts.dir", "/app/");
        configMap.put("working.dir", "/tmp/");
        configMap.put("entry.point", "end2end_1.poll");
        configMap.put("init.method", "init");
        configMap.put("private.settings", "{\"conf1\":\"value1\", \"conf2\":\"value2\"}");

        configMap.put("value.converter.schema.registry.url", "http://schema-registry:8081");

        String connectorConfigText = connectorConfigMapper.writeValueAsString(connectorMap);

        postConnector(connectorConfigText);

        try (KafkaConsumer<Long, String> consumer = getConsumer1()) {
            consumer.subscribe(Arrays.asList(topicName));
            List<ConsumerRecord<Long, String>> messages = drainLongString(consumer, 3);

            String firstValue = messages.get(0).value();
            Long firstKey = messages.get(0).key();

            Assertions.assertTrue(firstValue.contains("some string"));
            Assertions.assertTrue(firstKey > 0);
        }
    }

    @Test
    /*
     * Test topology: source connector --> full python message --> topic --> consumer 1 checks msg (String + JSON)
     */
    @SneakyThrows
    public void sourceConnectorToConsumerStringJSON() {
        String topicName = "test-topic-" + UUID.randomUUID();

        ObjectMapper connectorConfigMapper = new ObjectMapper();

        Map<String, Object> connectorMap = new HashMap<>();
        connectorMap.put("name", "pytools-test-2");
        connectorMap.put("config", new HashMap<>());

        Map<String, Object> configMap = (Map<String, Object>) connectorMap.get("config");
        configMap.put("connector.class", "io.confluent.pytools.PySourceConnector");
        configMap.put("kafka.topic", topicName);
        configMap.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configMap.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        configMap.put("value.converter.schemas.enable", "false");

        configMap.put("scripts.dir", "/app/");
        configMap.put("working.dir", "/tmp/");
        configMap.put("entry.point", "end2end_2.poll");
        configMap.put("init.method", "init");
        configMap.put("private.settings", "{\"conf1\":\"value1\", \"conf2\":\"value2\"}");

        configMap.put("value.converter.schema.registry.url", "http://schema-registry:8081");

        String connectorConfigText = connectorConfigMapper.writeValueAsString(connectorMap);

        postConnector(connectorConfigText);

        try (KafkaConsumer<String, JsonNode> consumer = getConsumer2()) {
            consumer.subscribe(Arrays.asList(topicName));
            List<ConsumerRecord<String, JsonNode>> messages = drainStringJSON(consumer, 1);

            String firstKey = messages.get(0).key();
            JsonNode firstValue = messages.get(0).value();

            Assertions.assertTrue(firstKey.contains("some string"));
            Assertions.assertTrue(firstValue.get("bool").booleanValue());
            Assertions.assertEquals(firstValue.get("str").asText(), "value1");
            Assertions.assertEquals(firstValue.get("long").asInt(), 1234);
            Assertions.assertEquals(firstValue.get("float").asDouble(), 1234.5);
        }
    }
}
