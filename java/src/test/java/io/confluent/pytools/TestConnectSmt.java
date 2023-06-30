package io.confluent.pytools;

import io.confluent.pytools.testutils.CommonTestUtils;
import io.confluent.pytools.testutils.ConnectStandalone;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.confluent.pytools.testutils.VerifiableSourceConnector;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class TestConnectSmt {
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
    }

    @AfterEach
    void cleanup() {
        commonTestUtils.stopKafkaContainer();
    }

    private Properties getTransformProperties() {
        Properties props = new Properties();

        props.put("transforms", "myTransform");
        props.put("transforms.myTransform.type", ConnectSmt.class.getName()); // io.confluent.pytools.ConnectSmt

        Path scriptsDirectory = Paths.get("src","test", "resources");
        props.put("transforms.myTransform.scripts.dir", scriptsDirectory.toString());

        props.put("transforms.myTransform.working.dir", tempDir.toString());

        props.put("transforms.myTransform.init.method", "init");
        props.put("transforms.myTransform.entry.point", "transform1.transform");
        props.put("transforms.myTransform.private.settings", "{\"conf1\":\"value1\", \"conf2\":\"value2\"}");

        return props;
    }

    @SneakyThrows
    @Test
    void withSourceTask() {

        ConnectStandalone connectStandalone = new ConnectStandalone(
                commonTestUtils.getConnectWorkerProperties(),
                commonTestUtils.getSourceTaskProperties(
                        getTransformProperties(), testTopic,
                        VerifiableSourceConnector.class));
        connectStandalone.start();

        commonTestUtils.consumeAtLeastXEvents(StringDeserializer.class, StringDeserializer.class,
                testTopic, 1);

        connectStandalone.stop();
    }

    private Properties getTransformPropertiesJSON() {
        Properties props = new Properties();

        props.put("transforms", "myTransform");
        props.put("transforms.myTransform.type", ConnectSmt.class.getName()); // io.confluent.pytools.ConnectSmt

        Path scriptsDirectory = Paths.get("src","test", "resources");
        props.put("transforms.myTransform.scripts.dir", scriptsDirectory.toString());

        props.put("transforms.myTransform.working.dir", tempDir.toString());

        props.put("transforms.myTransform.init.method", "init");
        props.put("transforms.myTransform.entry.point", "transform1.transform_json");
        props.put("transforms.myTransform.private.settings", "{\"conf1\":\"value1\", \"conf2\":\"value2\"}");

        return props;
    }

    @SneakyThrows
    @Test
    void withSourceTaskJSON() {

        ConnectStandalone connectStandalone = new ConnectStandalone(
                commonTestUtils.getConnectWorkerProperties(),
                commonTestUtils.getSourceTaskProperties(
                        getTransformPropertiesJSON(), testTopic,
                        VerifiableSourceConnector.class));
        connectStandalone.start();

        commonTestUtils.consumeAtLeastXEvents(StringDeserializer.class, StringDeserializer.class,
                testTopic, 1);

        connectStandalone.stop();
    }
}
