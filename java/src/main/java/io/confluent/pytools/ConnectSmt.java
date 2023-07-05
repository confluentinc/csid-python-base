package io.confluent.pytools;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;
@Slf4j
public class ConnectSmt<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Runs a python script to transform Kafka records";

    public static final String PYTHON_PATH_FIELD = "python.path";
    public static final String ENTRY_POINT_FIELD = "entry.point";
    public static final String SCRIPTS_DIR_FIELD = "scripts.dir";
    public static final String WORKING_DIR_FIELD = "working.dir";
    public static final String CONFIGURE_FIELD = "init.method";

    public static final String OFFLINE_INSTALL_FIELD = "offline.installation.dir";

    // transform entry point, same object/data is passed and returned
    // "kafka_record" = KafkaRecord python object --> allows to modify topic, partition, headers, key and value
    // "key_value" = Tuple(String) a tuple
    public static final String IO_FORMAT_FIELD = "io.format";

    public static final String SETTINGS_FIELD = "private.settings";

    /**
     * The format for entry points is driven by the way pemja works.
     * (and the organization of scripts inside the module)
     * - If the module doesn't have sub-modules, we'll import and call it this way:
     *      pyEnv.executePythonStatement("import arrow");
     *      Object res = pyEnv.callPythonMethod("arrow.utcnow");
     *   So the entry point should be provided as "arrow.utcnow"
     * - If there are sub-modules, we'll import and call it using an alias:
     *      pyEnv.executePythonStatement("import algorithms.strings as s");
     *      res = pyEnv.callPythonMethod("s.decode_string", "3[a]2[bc]");
     *   So the entry point should be provided as "algorithms.strings.decode_string"
     *   and we'll split it at the last dot and use an alias.
     */

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(PYTHON_PATH_FIELD, ConfigDef.Type.STRING,
                    "", new ConfigDef.NonNullValidator(),
                    ConfigDef.Importance.HIGH,
                    "Path of the python3 executable to build the virtual environment from. " +
                            "If empty, the default python3 executable from the system is taken.")
            .define(SCRIPTS_DIR_FIELD, ConfigDef.Type.STRING,
                    NO_DEFAULT_VALUE, new ConfigDef.NonNullValidator(),
                    ConfigDef.Importance.HIGH,
                    "The directory where the python scripts reside.")
            .define(WORKING_DIR_FIELD, ConfigDef.Type.STRING,
                    "", new ConfigDef.NonNullValidator(),
                    ConfigDef.Importance.HIGH,
                    "The directory where the virtual environment is built. " +
                            "If not passed, it will be created in the scripts dir.")
            .define(ENTRY_POINT_FIELD, ConfigDef.Type.STRING,
                    NO_DEFAULT_VALUE, new ConfigDef.NonNullValidator(),
                    ConfigDef.Importance.HIGH,
                    "The entry point (library + method name) for the (python) transform method.")
            .define(CONFIGURE_FIELD, ConfigDef.Type.STRING,
                    NO_DEFAULT_VALUE, new ConfigDef.NonNullValidator(),
                    ConfigDef.Importance.HIGH,
                    "The name of the (python) init method. " +
                            "(Called once when the SMT initializes.)")
            .define(SETTINGS_FIELD, ConfigDef.Type.STRING,
                    NO_DEFAULT_VALUE, new ConfigDef.NonNullValidator(),
                    ConfigDef.Importance.HIGH,
                    "A JSON string with private settings given to the init method.")
            .define(OFFLINE_INSTALL_FIELD, ConfigDef.Type.STRING,
                    "", new ConfigDef.NonNullValidator(),
                    ConfigDef.Importance.HIGH,
                    "The directory containing wheel/python packages for offline installation of " +
                            "the packages in the virtual environment.");

    private String jsonPrivateSettings;
    private PythonHost pythonHost;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // delete the workingDirectory (venv)?
    }

    @Override
    @SneakyThrows
    public void configure(Map<String, ?> props) {
        System.out.println("configuring SMT");
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

        String pythonExe = config.getString(PYTHON_PATH_FIELD);
        String entryPoint = config.getString(ENTRY_POINT_FIELD);
        String scriptsDir = config.getString(SCRIPTS_DIR_FIELD);
        String workingDir = config.getString(WORKING_DIR_FIELD);
        String initMethod = config.getString(CONFIGURE_FIELD);
        String localDependenciesDir = config.getString(OFFLINE_INSTALL_FIELD);

        jsonPrivateSettings = config.getString(SETTINGS_FIELD);

        System.out.println("initializing the python environment");

        String pythonExecutable = PyUtils.defaultPythonExecutablePath().toString();
        if (!pythonExe.equals("")) {
            pythonExecutable = pythonExe;
        }

        String workingDirectory = scriptsDir;
        if (!workingDir.equals("")) {
            workingDirectory = workingDir;
        }

        pythonHost = new PythonHost(pythonExecutable, Paths.get(scriptsDir).toFile(), entryPoint, workingDirectory, localDependenciesDir);

        // call a configure() function in python?
        if (!initMethod.equals("")) {
            pythonHost.callPythonMethod(initMethod, jsonPrivateSettings);
            System.out.println("calling the init method: " + initMethod);
        }
    }

    @Override
    public R apply(R record) {
        System.out.println("transforming 1 record");

        Object pyResult = pythonHost.callEntryPoint(toPython(record));
        if (pyResult == null) {
            System.out.println("null returned by python, message will be dropped");
            return null;
        }
        System.out.println("returned by python: " + pyResult.toString());

        R newRecord = fromPython(pyResult, record);
        System.out.println("after conversion fromPython: " + newRecord.toString());

        return newRecord;
    }

    public Object toPython(R record) {
        HashMap<String, Object> obj = new HashMap<>();
        obj.put("topic", record.topic());

        obj.put("key_schema", PyJavaIO.normalizedTypeName(record.keySchema()));
        obj.put("key", PyJavaIO.payloadTyped(record.key(), record.keySchema().toString()));

        obj.put("value_schema", PyJavaIO.normalizedTypeName(record.valueSchema()));
        obj.put("value", PyJavaIO.payloadTyped(record.value(), record.valueSchema().toString()));

        return obj;
    }

    public Object headersToPython(R record) {
        HashMap<String, String> headers = new HashMap<>();
        for (Header header: record.headers()) {
            headers.put(header.key(), header.value().toString());
        }
        return headers;
    }

    public R fromPython(Object pythonResult, R originalRecord) {
        try {
            HashMap<String, String> newRecordData = (HashMap<String, String>) pythonResult;

            // the python script cannot/shouldn't change the type of the key or value
            return originalRecord.newRecord(
                    newRecordData.get("topic"),
                    originalRecord.kafkaPartition(),
                    originalRecord.keySchema(),
                    PyJavaIO.matchingParse(originalRecord.keySchema(), newRecordData.get("key")),
                    originalRecord.valueSchema(),
                    PyJavaIO.matchingParse(originalRecord.valueSchema(), newRecordData.get("value")),
                    originalRecord.timestamp());
        } catch (Exception e) {
            System.out.println("Error processing returned value from python: " + e);
            return originalRecord;
        }
    }
}
