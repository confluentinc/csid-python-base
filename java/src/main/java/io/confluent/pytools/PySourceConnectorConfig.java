package io.confluent.pytools;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.NonNullValidator;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

public class PySourceConnectorConfig extends AbstractConfig {

    public static final String KAFKA_TOPIC_CONF = "kafka.topic";
    private static final String KAFKA_TOPIC_DOC = "Topic to write to";
    public static final String MAXINTERVAL_CONF = "max.interval";
    private static final String MAXINTERVAL_DOC = "Max interval between messages (ms)";
    public static final String ITERATIONS_CONF = "iterations";
    private static final String ITERATIONS_DOC = "Number of messages to send from each task, "
            + "or less than 1 for unlimited";
    public static final String RANDOM_SEED_CONF = "random.seed";
    private static final String RANDOM_SEED_DOC = "Numeric seed for generating random data. "
            + "Two connectors started with the same seed will deterministically produce the same data. "
            + "Each task will generate different data than the other tasks in the same connector.";

    public static final String PYTHON_PATH_CONF = "python.path";
    public static final String PYTHON_PATH_DOC = "Path of the python3 executable to build the virtual environment from. " +
            "If empty, the default python3 executable from the system is taken.";
    public static final String ENTRY_POINT_CONF = "entry.point";
    public static final String ENTRY_POINT_DOC = "The entry point (library + method name) for the (python) connector poll method.";
    public static final String SCRIPTS_DIR_CONF = "scripts.dir";
    public static final String SCRIPTS_DIR_DOC = "The directory where the python scripts reside.";
    public static final String WORKING_DIR_CONF = "working.dir";
    public static final String WORKING_DIR_DOC = "The directory where the virtual environment is built. " +
            "If not passed, it will be created in the scripts dir.";
    public static final String CONFIGURE_CONF = "init.method";
    public static final String CONFIGURE_DOC = "The name of the (python) init method. " +
            "(Called once when the connector task starts.)";

    public static final String OFFLINE_INSTALL_CONF = "offline.installation.dir";
    public static final String OFFLINE_INSTALL_DOC = "Offline installation directory";

    public static final String SETTINGS_CONF = "private.settings";
    public static final String SETTINGS_DOC = "A JSON string with private settings given to the init (task start) method.";

    public PySourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public PySourceConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(KAFKA_TOPIC_CONF, Type.STRING, Importance.HIGH, KAFKA_TOPIC_DOC)
                .define(MAXINTERVAL_CONF, Type.LONG, 500L, Importance.HIGH, MAXINTERVAL_DOC)
                .define(ITERATIONS_CONF, Type.INT, -1, Importance.HIGH, ITERATIONS_DOC)

                .define(PYTHON_PATH_CONF, Type.STRING, "", new NonNullValidator(), Importance.HIGH, PYTHON_PATH_DOC)
                .define(SCRIPTS_DIR_CONF, Type.STRING, NO_DEFAULT_VALUE, new NonNullValidator(), Importance.HIGH, SCRIPTS_DIR_DOC)
                .define(WORKING_DIR_CONF, Type.STRING, "", new NonNullValidator(), Importance.MEDIUM, WORKING_DIR_DOC)
                .define(ENTRY_POINT_CONF, Type.STRING, NO_DEFAULT_VALUE, new NonNullValidator(), Importance.HIGH, ENTRY_POINT_DOC)
                .define(CONFIGURE_CONF, Type.STRING, NO_DEFAULT_VALUE, new NonNullValidator(), Importance.MEDIUM, CONFIGURE_DOC)
                .define(SETTINGS_CONF, Type.STRING, NO_DEFAULT_VALUE, new NonNullValidator(), Importance.LOW, SETTINGS_DOC)
                .define(OFFLINE_INSTALL_CONF, Type.STRING, "", new NonNullValidator(), Importance.LOW, OFFLINE_INSTALL_DOC);
    }

    public String getKafkaTopic() {
        return this.getString(KAFKA_TOPIC_CONF);
    }
    public Long getMaxInterval() {
        return this.getLong(MAXINTERVAL_CONF);
    }
    public Integer getIterations() {
        return this.getInt(ITERATIONS_CONF);
    }
    public Long getRandomSeed() { return this.getLong(RANDOM_SEED_CONF);}

    public String getPythonPath() { return this.getString(PYTHON_PATH_CONF);}
    public String getScriptsDir() { return this.getString(SCRIPTS_DIR_CONF);}
    public String getWorkingDir() { return this.getString(WORKING_DIR_CONF);}
    public String getEntryPoint() { return this.getString(ENTRY_POINT_CONF);}
    public String getConfigureMethod() { return this.getString(CONFIGURE_CONF);}
    public String getPythonSettings() { return this.getString(SETTINGS_CONF);}
    public String getOfflineInstallPath() { return this.getString(OFFLINE_INSTALL_CONF);}
}