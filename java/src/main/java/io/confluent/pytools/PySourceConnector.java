package io.confluent.pytools;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PySourceConnector extends SourceConnector {

    private static Logger log = LoggerFactory.getLogger(PySourceConnector.class);
    private PySourceConnectorConfig config;
    private Map<String, String> props;

    @Override
    public String version() {
        return "0.1";
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            this.props = props;
            config = new PySourceConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConfigException(
                    "PySourceConnector connector could not start because of an error in the configuration: ",
                    e
            );
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PySourceConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskConfig = new HashMap<>(this.props);
            taskConfig.put(PySourceConnectorTask.TASK_ID, Integer.toString(i));
            taskConfigs.add(taskConfig);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return PySourceConnectorConfig.conf();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config config = super.validate(connectorConfigs);

        // skip further validations if any single config validations have failed
        try {
            this.config = new PySourceConnectorConfig(connectorConfigs);
        } catch (ConfigException e) {
            return config;
        }
        return config;
    }

    private ConfigValue getConfigValue(Config config, String configName) {
        return config.configValues().stream()
                .filter(value -> value.name().equals(configName))
                .findFirst().orElse(null);
    }
}