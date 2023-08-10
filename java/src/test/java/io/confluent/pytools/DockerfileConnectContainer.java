/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

package io.confluent.pytools;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DockerfileConnectContainer extends GenericContainer<DockerfileConnectContainer> {
    private static final int PORT = 8083;
    private static final String PLUGIN_PATH_CONTAINER = "/usr/share/java/";

    private static final String networkAlias = "connect";

    private static boolean verifyPathExists(String pathToTry) {
        return new File(pathToTry).exists();
    }

    private static Path connectDockerFilePath() {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        String pathToTry = "pytools-docker";
        if (verifyPathExists(pathToTry)) {
            return Paths.get(pathToTry);
        }
        pathToTry = "pytools-docker";
        if (verifyPathExists(pathToTry)) {
            return Paths.get(pathToTry);
        }
        throw new IllegalStateException("Could not find confluent-pytools directory");
    }

    public DockerfileConnectContainer(DockerImageName dockerImageName, KafkaContainer kafka, SchemaRegistryContainer schemaRegistry) {
        new GenericContainer<>(
            new ImageFromDockerfile().withFileFromPath(".", connectDockerFilePath()))
                .withNetwork(kafka.getNetwork()
                )
                .withEnv("CONNECT_BOOTSTRAP_SERVERS", String.format("%s:%d", kafka.getNetworkAliases().get(0), 9092))
                .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", networkAlias)
                .withEnv("CONNECT_REST_PORT", Integer.toString(PORT))
                .withEnv("CONNECT_GROUP_ID", "connect-1")
                .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect-config")
                .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect-offsets")
                .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect-status")
                .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "INFO")
                .withEnv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL",
                         String.format("http://%s:%s", schemaRegistry.getNetworkAliases().get(0), schemaRegistry.getExposedPorts().get(0)))
                .withEnv("CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL",
                         String.format("http://%s:%s", schemaRegistry.getNetworkAliases().get(0), schemaRegistry.getExposedPorts().get(0)))
                .withEnv("CONNECT_VALUE_CONVERTER", "io.confluent.connect.avro.AvroConverter")
                .withEnv("CONNECT_KEY_CONVERTER", "io.confluent.connect.avro.AvroConverter")
                .withEnv("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_PLUGIN_PATH", PLUGIN_PATH_CONTAINER)
                .withExposedPorts(PORT);
    }

    @Override
    protected void configure() {
        super.configure();
    }

    public DockerfileConnectContainer withPlugins(Set<String> plugins) {
        if (plugins == null) {
            return this;
        }
        plugins.forEach(this::withPlugins);
        return this;
    }

    public DockerfileConnectContainer withPlugins(String pluginPath) {
        Stream.of(new File(pluginPath).listFiles())
                .forEach(f -> this.withCopyFileToContainer(MountableFile.forHostPath(f.getPath()), PLUGIN_PATH_CONTAINER));
        return this;
    }

    /**
     * Searches through directory tree and find files that match includeFileNames while excluding excludeFileNames.
     * Additional filtering done to only include files that have filterPath in their full path.
     * For example - searching in root path of a project - include *-SNAPSHOT.jar, exclude - *-test?-*.jar, filterPath - /target/
     * will find all *-SNAPSHOT.jar build artifacts in /target/ folders while excluding -test-*.jar and -tests-*.jar files.
     * @param rootPathForSearch path to start the search at
     * @return List of File objects with results.
     */
    private static List<File> findSnapshotJarsInTargetFolders(String rootPathForSearch) {
        String includeFileNames = "confluent-pytools-*.jar";
        String excludeFileNames = "*-test?-*.jar";
        String filterPath = "/target/";
        return FileUtils.listFiles(new File(rootPathForSearch),
                        new WildcardFileFilter(includeFileNames) //includeFileNames filter
                                .and(new WildcardFileFilter(excludeFileNames).negate()), // excludeFileNames filter through negate()
                        TrueFileFilter.INSTANCE) //walk through all directories / subdirectories recursively
                .stream()
                //At the moment checks for filterPath (for example /target/) anywhere in files path - can be
                //tightened to only consider immediate parent directory.
                .filter(file -> file.toPath().toString().contains(filterPath)) // Only include results that have filterPath in their path
                .collect(Collectors.toList());
    }

    public DockerfileConnectContainer withSnapshotJars(String rootPathForSearch) {
        List<File> builtLibs = findSnapshotJarsInTargetFolders(rootPathForSearch);
        builtLibs.stream()
                .forEach(f -> this.withCopyFileToContainer(MountableFile.forHostPath(f.getPath()), "/etc/kafka-connect/jars/"));
        return this;
    }

    public String getEndpoint() {
        return String.format("http://%s:%d", getHost(), getMappedPort(PORT));
    }

}

