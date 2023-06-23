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
import java.time.Duration;
import java.util.List;
import java.util.UUID;

public class TestConnectSmt {
   private String testTopic;
    private final Charset charset = StandardCharsets.UTF_8;
    private final String transformClassName = "TestConnectSmt";
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

    @SneakyThrows
    @Test
    void withSourceTask() {

        ConnectStandalone connectStandalone = new ConnectStandalone(
                commonTestUtils.getConnectWorkerProperties(),
                commonTestUtils.getSourceTaskProperties(
                        commonTestUtils.getHeaderInjectTransformProperties(), testTopic,
                        VerifiableSourceConnector.class));
        connectStandalone.start();

        commonTestUtils.consumeAtLeastXEvents(StringDeserializer.class, StringDeserializer.class,
                testTopic, 1);

        connectStandalone.stop();
    }
/*

    @SneakyThrows
    @Test
    void testSMTCaptureWithHeaderCaptureUsedWithSinkTask() {

        ConnectStandalone connectStandalone = new ConnectStandalone(
                commonTestUtils.getConnectWorkerProperties(),
                commonTestUtils.getSinkTaskProperties(
                        commonTestUtils.getHeaderInjectTransformProperties(), testTopic));
        connectStandalone.start();

        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100)).until(
                connectStandalone::isRunning);

        String key = " {\"schema\":{\"type\":\"int32\",\"optional\":false},\"payload\":0}";
        String value = "{\"schema\":{\"type\":\"int64\",\"optional\":false},\"payload\":31}";
        commonTestUtils.produceSingleEvent(testTopic, key, value);

        commonTestUtils.waitUntil("Wait for traces", () -> instrumentation.waitForTraces(1).get(0).size() == 4);

        connectStandalone.stop();

        List<List<SpanData>> traces = instrumentation.waitForTraces(1);
        // Only checking first trace's third span - should be the SMT span.
        // Now that SinkTask is wired - first is producer send span, followed by consumer process, SMT and Sink task.
        assertSpan(traces.get(0).get(2), smt().withNameContaining(transformClassName)
                .withHeaders(charset, CAPTURED_PROPAGATED_HEADER));
    }

    private void assertSpan(SpanData actual, SpanAssertData expectations) {
        expectations.accept(OpenTelemetryAssertions.assertThat(actual));
    }
*/
}
