package io.confluent.pytools;

import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static io.confluent.pytools.PyJavaIO.getSchemaFromJavaClassName;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPyJavaIO {
    private static Stream<Arguments> provideClassToSchemaArgs() {
        return Stream.of(
                Arguments.of("java.lang.String", Schema.STRING_SCHEMA),
                Arguments.of("java.lang.Long", Schema.INT64_SCHEMA),
                Arguments.of("java.lang.Short", Schema.INT16_SCHEMA),
                Arguments.of("java.lang.Integer", Schema.INT32_SCHEMA),
                Arguments.of("java.lang.Double", Schema.FLOAT64_SCHEMA),
                Arguments.of("java.lang.Float", Schema.FLOAT32_SCHEMA),
                Arguments.of("java.lang.Boolean", Schema.BOOLEAN_SCHEMA),
                Arguments.of("java.lang.Byte", Schema.BYTES_SCHEMA),
                Arguments.of("[B", Schema.BYTES_SCHEMA)
        );
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("provideClassToSchemaArgs")
    void classToSchema(String className, Schema correspondingSchema) {
        assertSame(getSchemaFromJavaClassName(className), correspondingSchema);
    }
}
