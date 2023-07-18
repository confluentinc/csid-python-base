package io.confluent.pytools;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.SneakyThrows;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;


class TestPythonPollResult {
    @TempDir
    File tempDir;

    private static final String TOPIC = "test-topic-" + UUID.randomUUID();;
    private static final int NUM_MESSAGES = 10;
    private static final int MAX_INTERVAL_MS = 0;
    private static final int TASK_ID = 0;

    private Map<String, String> config;
    private PySourceConnectorTask task;
    private List<SourceRecord> records;
    private Map<String, Object> sourceOffsets;

    @BeforeEach
    void setUp() {
        config = new HashMap<>();
        records = new ArrayList<>();
        sourceOffsets = null;
    }

    @AfterEach
    void tearDown() {
        task.stop();
        task = null;
    }

    @SneakyThrows
    @Test
    void basic() {
        createPythonTask("init", "src_connector2.poll_basic_types");
        generateRecords(3);

        assertEquals(3, records.size());

        SourceRecord record = records.get(0);
        assertEquals(record.value(), "some string");
        assertEquals(record.key(), 1234L);
        assertTrue(record.key() instanceof java.lang.Long);

        record = records.get(1);
        assertEquals(record.value(), true);
        assertEquals(record.key(), 1234.5D);
        assertTrue(record.key() instanceof java.lang.Double);

        record = records.get(2);
        assertEquals(record.key(), 1234.5D);

        Struct value = (Struct)record.value();
        assertEquals(value.get("str"), "Hello");
        assertTrue(value.get("str") instanceof java.lang.String);

        assertEquals(value.get("bool"), true);
        assertTrue(value.get("bool") instanceof java.lang.Boolean);

        assertTrue(value.get("int") instanceof java.lang.Long);
        assertEquals(value.get("int"), 25L);

        assertEquals(value.get("float"), 1.0D);
        assertTrue(value.get("float") instanceof java.lang.Double);

        assertTrue(value.get("bytes") instanceof java.lang.Object);
    }

    @SneakyThrows
    @Test
    void bothObjects() {
        createPythonTask("init", "src_connector2.poll_key_and_value_both_objects");
        generateRecords(2);

        assertEquals(2, records.size());

        SourceRecord record = records.get(0);
        Struct value = (Struct)record.value();
        Struct key = (Struct)record.key();

        assertEquals(key.get("id"), 1234L);
        assertEquals(key.get("type"), "something");

        assertEquals(value.get("first_name"), "John");
        assertEquals(value.get("last_name"), "Doe");
        assertTrue(value.get("age") instanceof java.lang.Long);

        record = records.get(1);
        value = (Struct)record.value();
        key = (Struct)record.key();

        assertEquals(key.get("id"), 567L);
        assertEquals(key.get("type"), "else");

        assertEquals(value.get("first_name"), "Jane");
        assertEquals(value.get("last_name"), "Dolittle");
        assertTrue(value.get("age") instanceof java.lang.Long);
    }

    @SneakyThrows
    @Test
    void allTypesNoKey() {
        createPythonTask("init", "src_connector2.all_default_types");

        generateRecords(1);
        assertEquals(1, records.size());

        SourceRecord record = records.get(0);

        Struct value = (Struct)record.value();

        assertEquals(value.get("str"), "Hello");
        assertTrue(value.get("str") instanceof java.lang.String);

        assertEquals(value.get("bool"), true);
        assertTrue(value.get("bool") instanceof java.lang.Boolean);

        assertEquals(value.get("float"), 1.0D);
        assertTrue(value.get("float") instanceof java.lang.Double);

        assertTrue(value.get("bytes") instanceof java.lang.Object);

        assertTrue(value.get("int") instanceof java.lang.Long);
        assertEquals(value.get("int"), 25L);

        assertNull(record.key());
    }

    @SneakyThrows
    @Test
    void single() {
        createPythonTask("init", "src_connector2.single_item");
        generateRecords(1);
        assertEquals(1, records.size());

        SourceRecord record = records.get(0);
        assertEquals(record.value(), "Hello");
        assertNull(record.key());
    }

    @SneakyThrows
    @Test
    void invalid() {
        createPythonTask("init", "src_connector2.invalid_1");
        generateRecords(1);

        createPythonTask("init", "src_connector2.invalid_2");
        generateRecords(1);

        assertEquals(0, records.size());
    }

    private void generateRecords(int numMessages) throws Exception {
        records.clear();
        while (records.size() < numMessages) {
            List<SourceRecord> newRecords = task.poll();
            if (newRecords != null) {
                records.addAll(newRecords);
            } else {
                return;
            }
        }
    }

    private void createPythonTask(String initMethod, String entryPoint) {
        config.putIfAbsent(PySourceConnectorConfig.KAFKA_TOPIC_CONF, TOPIC);
        config.putIfAbsent(PySourceConnectorTask.TASK_ID, Integer.toString(TASK_ID));

        Path scriptsDirectory = Paths.get("src","test", "resources");
        config.putIfAbsent(PySourceConnectorConfig.SCRIPTS_DIR_CONF, scriptsDirectory.toString());
        config.putIfAbsent(PySourceConnectorConfig.WORKING_DIR_CONF, tempDir.toString());

        config.putIfAbsent(PySourceConnectorConfig.CONFIGURE_CONF, initMethod);
        config.putIfAbsent(PySourceConnectorConfig.ENTRY_POINT_CONF, entryPoint);
        config.putIfAbsent(PySourceConnectorConfig.SETTINGS_CONF, "{\"conf1\":\"value1\", \"conf2\":\"value2\"}");

        task = new PySourceConnectorTask();
        // Initialize an offsetStorageReader that returns mocked sourceOffsets.
        task.initialize(new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return config;
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    @Override
                    public <T> Map<String, Object> offset(final Map<String, T> partition) {
                        return offsets(Collections.singletonList(partition)).get(partition);
                    }

                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(
                            final Collection<Map<String, T>> partitions) {
                        if (sourceOffsets == null) {
                            return Collections.emptyMap();
                        }
                        return partitions
                                .stream()
                                .collect(Collectors.toMap(
                                        Function.identity(),
                                        ignored -> sourceOffsets
                                ));
                    }
                };
            }
        });
        task.start(config);
    }
}
