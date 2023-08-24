/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

package io.confluent.pytools;

import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.HttpStatus;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;


public class KafkaConnectBase extends KafkaBase {
    protected static final String CONNECT_IMAGE = "ldom/connect-with-devtools:latest"; // "confluentinc/cp-kafka-connect:7.4.1";
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectBase.class);
    @TempDir
    static Path libTempDir;
    private static ConnectContainer connect;

    @BeforeAll
    public static void dockerSetup() {
        network = Network.newNetwork();

        kafka = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE))
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://broker:9092,PLAINTEXT_HOST://localhost:9092")
                .withNetworkAliases("broker")
                .withEmbeddedZookeeper()
                .withNetwork(network);

        schemaRegistry = new SchemaRegistryContainer(DockerImageName.parse(SCHEMA_REGISTRY_IMAGE))
                .withNetwork(network)
                .withNetworkAliases("schema-registry")
                .withKafka(kafka)
                .dependsOn(kafka);


        connect = new ConnectContainer(DockerImageName.parse(CONNECT_IMAGE), kafka, schemaRegistry)
                .withNetworkAliases("connect")
                .withNetwork(network)
                .dependsOn(schemaRegistry)
                .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/confluent-hub-components,/etc/kafka-connect/jars/")
                .withCopyFileToContainer(MountableFile.forClasspathResource("connect-start.sh", 0777),
                        "/usr/share/connect-start.sh")
                .withCopyFileToContainer(MountableFile.forClasspathResource("end2end_1.py", 0777),
                        "/app/")
                .withCopyFileToContainer(MountableFile.forClasspathResource("end2end_2.py", 0777),
                        "/app/")
                .withCopyFileToContainer(MountableFile.forClasspathResource("end2end_3.py", 0777),
                        "/app/")
                .withCopyFileToContainer(MountableFile.forClasspathResource("speed_traps.py", 0777),
                        "/app/")
                .withCopyFileToContainer(MountableFile.forClasspathResource("requirements.txt", 0777),
                        "/app/")
                .withSnapshotJars("..") // was /Users/laurent/Repositories/csid-python-base
                .withPlugins(libTempDir.toString());

        Startables.deepStart(Stream.of(
                kafka,
                schemaRegistry,
                connect
        )).join();

    }

    protected void postConnector(String connectorConfig) {
        given()
                .log().all()
                .contentType(ContentType.JSON)
                .accept(ContentType.JSON)
                .body(connectorConfig)
                .when()
                .post(connect.getEndpoint() + "/connectors")
                .andReturn()
                .then()
                .log().all()
                .statusCode(HttpStatus.SC_CREATED);
    }
}
