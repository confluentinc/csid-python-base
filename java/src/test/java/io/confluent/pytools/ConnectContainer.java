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

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConnectContainer extends GenericContainer<ConnectContainer> {
    private static final int PORT = 8083;
    private static final String PLUGIN_PATH_CONTAINER = "/usr/share/java/";

    private static final String networkAlias = "connect";

    public ConnectContainer(DockerImageName dockerImageName, KafkaContainer kafka, SchemaRegistryContainer schemaRegistry) {
        super(dockerImageName);
        withNetwork(kafka.getNetwork());
        addEnv("CONNECT_BOOTSTRAP_SERVERS", String.format("%s:%d", kafka.getNetworkAliases().get(0), 9092));
        addEnv("CONNECT_REST_ADVERTISED_HOST_NAME", networkAlias);
        addEnv("CONNECT_REST_PORT", Integer.toString(PORT));
        addEnv("CONNECT_GROUP_ID", "connect-1");
        addEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect-config");
        addEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect-offsets");
        addEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect-status");
        addEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1");
        addEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1");
        addEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1");
        addEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "INFO");
        addEnv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL",
                String.format("http://%s:%s", schemaRegistry.getNetworkAliases().get(0), schemaRegistry.getExposedPorts().get(0)));
        addEnv("CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL",
                String.format("http://%s:%s", schemaRegistry.getNetworkAliases().get(0), schemaRegistry.getExposedPorts().get(0)));
        addEnv("CONNECT_VALUE_CONVERTER", "io.confluent.connect.avro.AvroConverter");
        addEnv("CONNECT_KEY_CONVERTER", "io.confluent.connect.avro.AvroConverter");
        addEnv("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        addEnv("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        addEnv("CONNECT_PLUGIN_PATH", PLUGIN_PATH_CONTAINER);
        withExposedPorts(PORT);
    }

    @Override
    protected void configure() {
        super.configure();
    }

    public ConnectContainer withPlugins(Set<String> plugins) {
        if (plugins == null) {
            return this;
        }
        plugins.forEach(this::withPlugins);
        return this;
    }

    public ConnectContainer withPlugins(String pluginPath) {
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

    public ConnectContainer withSnapshotJars(String rootPathForSearch) {
        List<File> builtLibs = findSnapshotJarsInTargetFolders(rootPathForSearch);
        builtLibs.stream()
                .forEach(f -> this.withCopyFileToContainer(MountableFile.forHostPath(f.getPath()), "/etc/kafka-connect/jars/"));
        return this;
    }

    public String getEndpoint() {
        return String.format("http://%s:%d", getHost(), getMappedPort(PORT));
    }

}

