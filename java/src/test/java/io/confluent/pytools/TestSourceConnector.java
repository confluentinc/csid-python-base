package io.confluent.pytools;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestSourceConnector {

    private static final String TOPIC = "my-topic";
    private static final int NUM_MESSAGES = 10;
    private static final int MAX_INTERVAL_MS = 100;

    private Map<String, String> config;
    private PySourceConnector connector;

    @BeforeEach
    void setUp() throws Exception {
        config = new HashMap<>();
        config.put(PySourceConnectorConfig.KAFKA_TOPIC_CONF, TOPIC);
        config.put(PySourceConnectorConfig.ITERATIONS_CONF, Integer.toString(NUM_MESSAGES));
        config.put(PySourceConnectorConfig.MAXINTERVAL_CONF, Integer.toString(MAX_INTERVAL_MS));
        connector = new PySourceConnector();
    }

    @AfterEach
    void tearDown() throws Exception {
        connector.stop();
    }

    @Test
    void shouldCreateTasks() {
        connector.start(config);

        assertTaskConfigs(1);
        assertTaskConfigs(2);
        assertTaskConfigs(4);
        assertTaskConfigs(10);
        for (int i=0; i!=100; ++i) {
            assertTaskConfigs(0);
        }
    }

    protected void assertTaskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = connector.taskConfigs(maxTasks);
        assertEquals(maxTasks, taskConfigs.size());
        // All task configs should match the connector config
        for (int i = 0; i < taskConfigs.size(); i++) {
            Map<String, String> taskConfig = taskConfigs.get(i);
            Map<String, String> expectedTaskConfig = new HashMap<>(config);
            expectedTaskConfig.put(PySourceConnectorTask.TASK_ID, Integer.toString(i));
            assertEquals(expectedTaskConfig, taskConfig);
        }
    }
}