package io.confluent.pytools;

import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.pytools.testutils.*;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.awaitility.Awaitility.await;

public class TestPyConnectSmt {
   private String testTopic;
    CommonTestUtils commonTestUtils;
    @TempDir
    File tempDir;

    @BeforeAll
    public static void setupAll() {
    }

    @BeforeEach
    void setup() {
        testTopic = "test-topic-" + UUID.randomUUID();
        commonTestUtils = new CommonTestUtils(tempDir.getAbsolutePath());
        commonTestUtils.startKafkaContainer();
        commonTestUtils.startSchemaRegistryContainer(commonTestUtils.getKafkaContainer());
    }

    @AfterEach
    void cleanup() {
        commonTestUtils.stopSchemaRegistryContainer();
        commonTestUtils.stopKafkaContainer();
    }

    private Properties getSRProperties() {
        Properties props = new Properties();
        props.put("value.converter.schema.registry.url", "http://localhost:8081");
        return props;
    }

    private Properties getTransformProperties(String methodName) {
        Properties props = new Properties();

        props.put("transforms", "myTransform");
        props.put("transforms.myTransform.type", PyConnectSmt.class.getName()); // io.confluent.pytools.PyConnectSmt

        Path scriptsDirectory = Paths.get("src","test", "resources");
        props.put("transforms.myTransform.scripts.dir", scriptsDirectory.toString());

        props.put("transforms.myTransform.working.dir", tempDir.toString());

        props.put("transforms.myTransform.init.method", "init");
        props.put("transforms.myTransform.entry.point", "transform1." + methodName);
        props.put("transforms.myTransform.private.settings", "{\"conf1\":\"value1\", \"conf2\":\"value2\"}");

        return props;
    }

    @SneakyThrows
    @Test
    void withSourceTask() {

        ConnectStandalone connectStandalone = new ConnectStandalone(
                commonTestUtils.getConnectWorkerProperties(),
                commonTestUtils.getSourceTaskProperties(
                        getTransformProperties("transform"), testTopic,
                        VerifiableSourceConnector.class));
        connectStandalone.start();

        List<ConsumerRecord> records = commonTestUtils.consumeAtLeastXEvents(StringDeserializer.class, StringDeserializer.class,
                testTopic, 1);

        Assertions.assertTrue(records.get(0).value().toString().contains("Modified from python --> Value 0"));

        connectStandalone.stop();
    }

    @SneakyThrows
    @Test
    void withSourceTaskJSON() {

        ConnectStandalone connectStandalone = new ConnectStandalone(
                commonTestUtils.getConnectWorkerProperties(getSRProperties(), JsonSchemaConverter.class.getName()),
                commonTestUtils.getSourceTaskProperties(
                        getTransformProperties("transform_json"), testTopic,
                        VerifiableSourceConnectorJSON.class));
        connectStandalone.start();

        List<ConsumerRecord> records = commonTestUtils.consumeAtLeastXEvents(StringDeserializer.class,
                StringDeserializer.class,
                testTopic, 1);

        Assertions.assertTrue(records.get(0).value().toString().contains("Modified from python"));

        connectStandalone.stop();
    }

    @SneakyThrows
    @Test
    void dropMessages() {

        ConnectStandalone connectStandalone = new ConnectStandalone(
                commonTestUtils.getConnectWorkerProperties(),
                commonTestUtils.getSourceTaskProperties(
                        getTransformProperties("drop_messages"), testTopic,
                        VerifiableSourceConnectorJSON.class));
        connectStandalone.start();

        Assertions.assertThrowsExactly(ConditionTimeoutException.class, () -> {
            List<ConsumerRecord> records = commonTestUtils.consumeAtLeastXEvents(StringDeserializer.class,
                    StringDeserializer.class,
                    testTopic, 1);
        });

        connectStandalone.stop();
    }

    @SneakyThrows
    @Test
    void withSinkTaskJSON() {

        ConnectStandalone connectStandalone = new ConnectStandalone(
                commonTestUtils.getConnectWorkerProperties(),
                commonTestUtils.getSinkTaskProperties(getTransformProperties("transform"), testTopic));

        connectStandalone.start();

        Thread.sleep(10000); // give time to the python env to build

        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100)).until(connectStandalone::isRunning);

        SimpleProducer producerThread = new SimpleProducer(testTopic, commonTestUtils);
        producerThread.start();

        List<ConsumerRecord> records = commonTestUtils.consumeAtLeastXEvents(StringDeserializer.class,
                StringDeserializer.class,
                testTopic, 1);

        Thread.sleep(30000);
        connectStandalone.stop();
    }


}
