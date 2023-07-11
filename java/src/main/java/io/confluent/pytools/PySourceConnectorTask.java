package io.confluent.pytools;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PySourceConnectorTask extends SourceTask {

    static final Logger log = LoggerFactory.getLogger(PySourceConnectorTask.class);

    private static final Schema DEFAULT_KEY_SCHEMA = Schema.OPTIONAL_STRING_SCHEMA;
    public static final String TASK_ID = "task.id";
    public static final String TASK_GENERATION = "task.generation";
    public static final String CURRENT_ITERATION = "current.iteration";
    public static final String RANDOM_SEED = "random.seed";

    private String topic;
    private long maxInterval;
    private int maxRecords;
    private long count = 0L;
    private int taskId;
    private Map<String, Object> sourcePartition;
    private long taskGeneration;
    private final Random random = new Random();

    private PythonHost pythonHost;
    private String jsonPrivateSettings;
    private String scriptName;


    @Override
    public String version() {
        return "0.1";
    }

    @SneakyThrows
    @Override
    public void start(Map<String, String> props) {
        System.out.println("task.start()");
        PySourceConnectorConfig config = new PySourceConnectorConfig(props);

        String pythonExe = config.getPythonPath();
        String scriptsDir = config.getScriptsDir();
        String workingDir = config.getWorkingDir();
        String entryPoint = config.getEntryPoint();
        scriptName = entryPoint; // TODO extract main method name
        String initMethod = config.getConfigureMethod();
        jsonPrivateSettings = config.getPythonSettings();
        String localDependenciesDir = config.getOfflineInstallPath();

        topic = config.getKafkaTopic();

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

        maxInterval = config.getMaxInterval();
        maxRecords = config.getIterations();

        taskGeneration = 0;
        taskId = Integer.parseInt(props.get(TASK_ID));
        sourcePartition = Collections.singletonMap(TASK_ID, taskId);

        Map<String, Object> offset = context.offsetStorageReader().offset(sourcePartition);
        if (offset != null) {
            //  The offset as it is stored contains our next state, so restore it as-is.
            taskGeneration = ((Long) offset.get(TASK_GENERATION)).intValue();
            count = ((Long) offset.get(CURRENT_ITERATION));
            random.setSeed((Long) offset.get(RANDOM_SEED));
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        System.out.println("task.poll()");

        ArrayList<HashMap<String, HashMap<String, Object>>> pyResults = (ArrayList<HashMap<String, HashMap<String, Object>>>)pythonHost.callEntryPoint();
        if (pyResults == null) {
            System.out.println("null returned by python, message will be dropped");
            return null;
        }
        System.out.println("returned by python: " + pyResults.toString());

/*
        if (maxInterval > 0) {
            try {
                Thread.sleep((long) (maxInterval * Math.random()));
            } catch (InterruptedException e) {
                Thread.interrupted();
                return null;
            }
        }

        // Key + Value
        SchemaAndValue key = new SchemaAndValue(DEFAULT_KEY_SCHEMA, null);
        String formattedString = String.format("%.02f", random.nextFloat());
        SchemaAndValue value = new SchemaAndValue(DEFAULT_KEY_SCHEMA, formattedString);

        if (maxRecords > 0 && count >= maxRecords) {
            throw new ConnectException(
                    String.format("Stopping connector: generated the configured %d number of messages", count)
            );
        }
*/

        // Re-seed the random each time so that we can save the seed to the source offsets.
        long seed = random.nextLong();
        random.setSeed(seed);

        // The source offsets will be the values that the next task lifetime will restore from
        // Essentially, the "next" state of the connector after this loop completes
        Map<String, Object> sourceOffset = new HashMap<>();
        // The next lifetime will be a member of the next generation.
        sourceOffset.put(TASK_GENERATION, taskGeneration + 1);
        // We will have produced this record
        sourceOffset.put(CURRENT_ITERATION, count + 1);
        // This is the seed that we just re-seeded for our own next iteration.
        sourceOffset.put(RANDOM_SEED, seed);

        final ConnectHeaders headers = new ConnectHeaders();
        headers.addLong(TASK_GENERATION, taskGeneration);
        headers.addLong(TASK_ID, taskId);
        headers.addLong(CURRENT_ITERATION, count);

        final List<SourceRecord> records = new ArrayList<>();
        for (HashMap<String, HashMap<String, Object>> result: pyResults) {
            records.add(PyJavaIO.pollResultToSourceRecord(result, sourcePartition, sourceOffset, topic, headers, scriptName));
        }
        count += records.size();
        return records;
    }

    @Override
    public void stop() {
        System.out.println("task.stop()");
        // TODO create config + call a python method
    }
}
