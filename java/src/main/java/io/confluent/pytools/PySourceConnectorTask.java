package io.confluent.pytools;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.SneakyThrows;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PySourceConnectorTask extends SourceTask {

    static final Logger log = LoggerFactory.getLogger(PySourceConnectorTask.class);

    public static final String TASK_ID = "task.id";

    private String topic;
    private int taskId;
    private Map<String, Object> sourcePartition;

    private PythonHost pythonHost;
    private String jsonPrivateSettings;
    private String scriptName;

    private Map<String, Object> offsets;


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
        taskId = Integer.parseInt(props.get(TASK_ID));
        sourcePartition = Collections.singletonMap(TASK_ID, taskId);

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

        offsets = context.offsetStorageReader().offset(sourcePartition);
        if (offsets == null) {
            offsets = new HashMap<>();
        }

        // call a configure() function in python?
        if (!initMethod.equals("")) {
            pythonHost.callPythonMethod(initMethod, jsonPrivateSettings, offsets);
            System.out.println("calling the init method: " + initMethod);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        System.out.println("task.poll()");

        Object uncastResults = pythonHost.callEntryPoint(offsets);
        if (uncastResults == null) {
            System.out.println("null returned by python, message(s) will be dropped");
            return null;
        }
        System.out.println("returned by python: " + uncastResults.toString());

        // we allow to receive a list or just a single element
        ArrayList<HashMap<String, HashMap<String, Object>>> pyResults = new ArrayList<>();;
        if (uncastResults instanceof ArrayList) {
            pyResults = (ArrayList<HashMap<String, HashMap<String, Object>>>)uncastResults;
        } else if (uncastResults instanceof HashMap) {
            pyResults.add((HashMap<String, HashMap<String, Object>>)uncastResults);
        } else {
            System.out.println("Invalid data type returned by python, message(s) will be dropped");
            return null; // TODO check if we need to throw an exception instead
        }

/*
        if (maxRecords > 0 && count >= maxRecords) {
            throw new ConnectException(
                    String.format("Stopping connector: generated the configured %d number of messages", count)
            );
        }
*/

        // refresh the offsets from the last 'offset' key of the py results
        offsets = new HashMap<>();
        offsets.put("latest", PythonPollResult.getLatestOffsetFromBatch(pyResults));

        final ConnectHeaders headers = new ConnectHeaders();
        headers.addLong(TASK_ID, taskId);

        final List<SourceRecord> records = new ArrayList<>();
        for (HashMap<String, HashMap<String, Object>> rawResult: pyResults) {
            PythonPollResult pyResult = new PythonPollResult(rawResult, scriptName);
            records.add(pyResult.toSourceRecord(sourcePartition, offsets, topic, headers));
        }
        return records;
    }

    @Override
    public void stop() {
        System.out.println("task.stop()");
        // TODO create config + call a python method
    }

    public Map<String, Object> getOffsets() {
        return offsets;
    }
}
