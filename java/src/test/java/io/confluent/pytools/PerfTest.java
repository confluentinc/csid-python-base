package io.confluent.pytools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;


public class PerfTest extends KafkaConnectBase {
     @Test
     @Disabled
    /*
     * Test topology: source connector --> full python message --> topic --> consumer 1 checks msg (String + JSON)
     */
    @SneakyThrows
    public void sourceConnectorToConsumer() {
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
        configMap.put("entry.point", "speed_traps.poll");
        configMap.put("private.settings", "{\"conf1\":\"value1\", \"conf2\":\"value2\"}");

        configMap.put("value.converter.schema.registry.url", "http://schema-registry:8081");

        String connectorConfigText = connectorConfigMapper.writeValueAsString(connectorMap);

        postConnector(connectorConfigText);

        try (KafkaConsumer<String, JsonNode> consumer = getConsumer2()) {
            consumer.subscribe(Arrays.asList(topicName));
            while (true) {
                List<ConsumerRecord<String, JsonNode>> messages = consumeStringJSON(consumer, 1);

                JsonNode firstValue = messages.get(0).value();
                String firstKey = messages.get(0).key();
            }
        }
    }
}
