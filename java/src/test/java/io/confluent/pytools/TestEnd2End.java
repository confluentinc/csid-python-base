package io.confluent.pytools;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestEnd2End extends KafkaConnectBase {
    @Disabled
    @Test
    /*
     * Test topology: source connector --> full python message --> topic --> consumer 1 checks msg
     */
    @SneakyThrows
    public void sourceConnectorToConsumer() {
        String topicName = "test-topic-" + UUID.randomUUID();

        ObjectMapper connectorConfigMapper = new ObjectMapper();

        Map<String, Object> connectorMap = new HashMap<>();
        connectorMap.put("name", "pytools-test");
        connectorMap.put("config", new HashMap<>());

        Map<String, Object> configMap = (Map<String, Object>) connectorMap.get("config");
        configMap.put("connector.class", "io.confluent.pytools.PySourceConnector");
        configMap.put("kafka.topic", topicName);
        configMap.put("key.converter", "org.apache.kafka.connect.converters.LongConverter");
        configMap.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configMap.put("value.converter.schemas.enable", "false");

        configMap.put("scripts.dir", "/app/");
        configMap.put("working.dir", "/tmp/");
        configMap.put("entry.point", "end2end.poll");
        configMap.put("init.method", "init");
        configMap.put("private.settings", "{\"conf1\":\"value1\", \"conf2\":\"value2\"}");

        configMap.put("value.converter.schema.registry.url", "http://schema-registry:8081");

        String connectorConfigText = connectorConfigMapper.writeValueAsString(connectorMap);

        postConnector(connectorConfigText);

        try (KafkaConsumer<String, String> consumer = getConsumer()) {
            consumer.subscribe(Arrays.asList(topicName));
            List<ConsumerRecord<String, String>> messages = drain(consumer, 10);

            String firstValue = messages.get(0).value();
            Assertions.assertTrue(firstValue.contains("some string"));
        }

    }

}
