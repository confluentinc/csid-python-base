/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

package io.confluent.pytools;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.StringReader;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;


public class KafkaBase {
    protected static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.4.1";
    protected static final String SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:7.4.1";
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectBase.class);


    protected static final String KEY_DATA = "key ";
    protected static final String VALUE_DATA = "value ";

    protected static Network network;
    protected static KafkaContainer kafka;
    protected static SchemaRegistryContainer schemaRegistry;

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

        Startables.deepStart(Stream.of(
                kafka,
                schemaRegistry
        )).join();

    }

    protected KafkaConsumer<String, String> getConsumer() {
        return new KafkaConsumer<>(
                new HashMap<>() {{
                    put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
                    put(ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID());
                    put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                    put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
                    put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
                }},
                new StringDeserializer(), new StringDeserializer());
    }

    protected KafkaConsumer<Long, String> getConsumer1() {
        return new KafkaConsumer<>(
                new HashMap<>() {{
                    put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
                    put(ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID());
                    put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                    put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer");
                    put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
                }},
                new LongDeserializer(), new StringDeserializer());
    }

    protected KafkaConsumer<String, byte[]> getEncryptionConsumer(String plaintextProperties) {
        Properties props = convertStringToProps(plaintextProperties);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<>(props); // , new StringDeserializer(), new SecuredStringDeserializer());
    }

    protected KafkaConsumer<String, JsonNode> getEncryptionJSONConsumer(String plaintextProperties) {
        Properties props = convertStringToProps(plaintextProperties);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<>(props); // , new StringDeserializer(), new SecuredStringDeserializer());
    }

    @SneakyThrows
    private Properties convertStringToProps(String plaintextProperties) {
        Properties props = new Properties();
        props.load(new StringReader(plaintextProperties));
        return props;
    }


    protected List<ConsumerRecord<String, String>> drain(
            KafkaConsumer<String, String> consumer,
            int expectedRecordCount) {

        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();

        Unreliables.retryUntilTrue(200, TimeUnit.SECONDS, () -> {
            consumer.poll(Duration.ofMillis(2000))
                    .iterator()
                    .forEachRemaining(allRecords::add);

            return allRecords.size() == expectedRecordCount;
        });
        LOGGER.info("Received records:\n{}", allRecords);
        return allRecords;
    }

    protected List<ConsumerRecord<Long, String>> drainLongString(
            KafkaConsumer<Long, String> consumer,
            int expectedRecordCount) {

        List<ConsumerRecord<Long, String>> allRecords = new ArrayList<>();

        Unreliables.retryUntilTrue(200, TimeUnit.SECONDS, () -> {
            consumer.poll(Duration.ofMillis(2000))
                    .iterator()
                    .forEachRemaining(allRecords::add);

            return allRecords.size() == expectedRecordCount;
        });
        LOGGER.info("Received records:\n{}", allRecords);
        return allRecords;
    }

/*
    private void addRecord(ConsumerRecord r) {

    }
*/

    protected List<ConsumerRecord<String, byte[]>> drainEncrypted(
            KafkaConsumer<String, byte[]> consumer,
            int expectedRecordCount) {

        List<ConsumerRecord<String, byte[]>> allRecords = new ArrayList<>();

        Unreliables.retryUntilTrue(200, TimeUnit.SECONDS, () -> {
            consumer.poll(Duration.ofMillis(2000))
                    .iterator()
                    .forEachRemaining(allRecords::add);
            //.forEachRemaining(addRecord);

            return allRecords.size() == expectedRecordCount;
        });
        LOGGER.info("Received records:\n{}", allRecords);
        return allRecords;
    }

    protected ConsumerRecords<String, JsonNode> drainJSONEncrypted(
            KafkaConsumer<String, JsonNode> consumer,
            int expectedRecordCount) {

        ConsumerRecords<String, JsonNode> records = null;

        try {
            while (true) {
                records = consumer.poll(1000);
            }
        } finally {
            consumer.close();
            LOGGER.info("Received records:\n{}", records);
            return records;
        }

//        List<ConsumerRecord<String, JsonNode>> allRecords = new ArrayList<>();

//        Unreliables.retryUntilTrue(200, TimeUnit.SECONDS, () -> {
/*
            consumer.poll(Duration.ofMillis(2000))
                    .iterator()
                    .forEachRemaining(allRecords::add);
*/
            //.forEachRemaining(addRecord);

//            return allRecords.size() == expectedRecordCount;
//        });
//        LOGGER.info("Received records:\n{}", records);
//        return records;
    }

/*
    protected KafkaProducer<String, String> getProducer() {
        return new KafkaProducer<>(
                new HashMap<String, Object>() {{
                    put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
                    put(ProducerConfig.CLIENT_ID_CONFIG, "prod-" + UUID.randomUUID());
                }},
                new StringSerializer(), new StringSerializer());
    }
*/

    protected KafkaProducer<String, byte[]> getEncryptionProducer(String plaintextProperties) {
        Properties props = convertStringToProps(plaintextProperties);
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "prod-" + UUID.randomUUID());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }

    protected void produce(
            KafkaProducer<String, byte[]> producer,
            String topicName,
            int recordCount) {


        for (int i = 0; i < recordCount; i++) {
            String key = KEY_DATA + i;
            String message = VALUE_DATA + i;

            producer.send(new ProducerRecord<>(topicName, key, message.getBytes()));
        }
        producer.close();
    }

    public String getTopicName() {
        return UUID.randomUUID().toString();
    }

}
