package io.confluent.pytools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;


class TestSourceConnectorTask {

    private static final String TOPIC = "my-topic";
    private static final int NUM_MESSAGES = 100;
    private static final int MAX_INTERVAL_MS = 0;
    private static final int TASK_ID = 0;

    private Map<String, String> config;
    private PySourceConnectorTask task;
    private List<SourceRecord> records;
    private Schema expectedValueConnectSchema;
    private Schema expectedKeyConnectSchema;
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


    @Test
    void shouldRestoreFromSourceOffsets() throws Exception {
        // Give the task an arbitrary source offset
        sourceOffsets = new HashMap<>();
        sourceOffsets.put(PySourceConnectorTask.RANDOM_SEED, 100L);
        sourceOffsets.put(PySourceConnectorTask.CURRENT_ITERATION, 50L);
        sourceOffsets.put(PySourceConnectorTask.TASK_GENERATION, 0L);
        createTask();

        // poll once to advance the generator
        SourceRecord firstPoll = task.poll().get(0);
        // poll a second time to predict the future
        SourceRecord pollA = task.poll().get(0);
        // extract the offsets after the first poll to restore to the next task instance
        //noinspection unchecked
        sourceOffsets = (Map<String, Object>) firstPoll.sourceOffset();
        createTask();
        // poll once after the restore
        SourceRecord pollB = task.poll().get(0);

        // the generation should have incremented, but the remaining details of the record should be identical
        assertEquals(1L, pollA.sourceOffset().get(PySourceConnectorTask.TASK_GENERATION));
        assertEquals(2L, pollB.sourceOffset().get(PySourceConnectorTask.TASK_GENERATION));
        assertEquals(pollA.sourceOffset().get(PySourceConnectorTask.TASK_ID), pollB.sourceOffset().get(PySourceConnectorTask.TASK_ID));
        assertEquals(pollA.sourceOffset().get(PySourceConnectorTask.CURRENT_ITERATION), pollB.sourceOffset().get(PySourceConnectorTask.CURRENT_ITERATION));
        assertEquals(pollA.sourcePartition(), pollB.sourcePartition());
        assertEquals(pollA.valueSchema(), pollB.valueSchema());
        assertEquals(pollA.value(), pollB.value());
    }

    @Test
    void shouldInjectHeaders()  throws Exception {
        createTask();
        generateRecords();
        for (SourceRecord record : records) {
            assertEquals((long) TASK_ID, record.headers().lastWithName(PySourceConnectorTask.TASK_ID).value());
            assertEquals(0L, record.headers().lastWithName(PySourceConnectorTask.TASK_GENERATION).value());
            assertNotNull(record.headers().lastWithName(PySourceConnectorTask.CURRENT_ITERATION));
        }
    }

    @Test
    void shouldFailToGenerateMoreRecordsThanSpecified() throws Exception {
        // Generate the expected number of records
        createTask();
        generateRecords();

        // Attempt to get another batch of records, but the task is expected to fail
        try {
            task.poll();
            fail("Expected poll to fail");
        } catch (ConnectException e) {
            // expected
        }
    }

    private void generateRecords() throws Exception {
        records.clear();
        while (records.size() < NUM_MESSAGES) {
            List<SourceRecord> newRecords = task.poll();
            assertNotNull(newRecords);
            assertEquals(1, newRecords.size());
            records.addAll(newRecords);
        }
    }

    private boolean isConnectInstance(Object value, Schema expected) {
        try {
            ConnectSchema.validateValue(expected, value);
        } catch (DataException e) {
            return false;
        }
        return true;
    }

    private void createTask() {
        config.putIfAbsent(PySourceConnectorConfig.KAFKA_TOPIC_CONF, TOPIC);
        config.putIfAbsent(PySourceConnectorConfig.ITERATIONS_CONF, Integer.toString(NUM_MESSAGES));
        config.putIfAbsent(PySourceConnectorConfig.MAXINTERVAL_CONF, Integer.toString(MAX_INTERVAL_MS));
        config.putIfAbsent(PySourceConnectorTask.TASK_ID, Integer.toString(TASK_ID));

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
