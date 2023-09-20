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


public class TestPiiSmt {
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

    private Properties getTransformProperties() {
        Properties props = new Properties();

        props.put("transforms", "myTransform");
        props.put("transforms.myTransform.type", PyConnectSmt.class.getName()); // io.confluent.pytools.PyConnectSmt

        Path scriptsDirectory = Paths.get("src","test", "resources");
        props.put("transforms.myTransform.python.path", "python3.10");
        props.put("transforms.myTransform.scripts.dir", Paths.get(scriptsDirectory.toString(), "pii").toString());

        props.put("transforms.myTransform.working.dir", tempDir.toString());

        props.put("transforms.myTransform.init.method", "init");
        props.put("transforms.myTransform.entry.point", "pii_smt.transform");
        props.put("transforms.myTransform.private.settings", "{\"models\": \"en_core_web_sm\", \"languages\": \"en\"}");

        return props;
    }

    @SneakyThrows
    @Test
    void withSourceTask() {

        ConnectStandalone connectStandalone = new ConnectStandalone(
                commonTestUtils.getConnectWorkerProperties(),
                commonTestUtils.getSourceTaskProperties(
                        getTransformProperties(), testTopic,
                        FlexibleSourceConnector.class));
        connectStandalone.start();

        List<ConsumerRecord> records = commonTestUtils.consumeAtLeastXEvents(StringDeserializer.class, StringDeserializer.class,
                testTopic, 1);

        Assertions.assertTrue(records.get(0).value().toString().contains("<LOCATION>"));

        connectStandalone.stop();
    }
}
