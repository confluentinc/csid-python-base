package io.confluent.pytools;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TestPythonEnvironmentPII {
    @SneakyThrows
    @Test
    void venvInstallAndRunPII() {
        // use volatile java temp dir instead of /tmp/
        String tmpDir = Files.createTempDirectory(null).toFile().getAbsolutePath();
        System.out.println("Temp Directory: " + tmpDir);

        String defaultPythonPath = PyUtils.defaultPythonExecutablePath("python3.13").toString();
        Path scriptsDirectory = Paths.get("src","test", "resources", "pii");
        PythonHost pythonHost = new PythonHost(defaultPythonPath, scriptsDirectory.toFile(),
                "pii_smt.anonymize", tmpDir, null);

        Properties props = new Properties();
        props.put("models", "en_core_web_sm");
        props.put("languages", "en");

        String propsJSON = "{\"models\": \"en_core_web_sm\", \"languages\": \"en\"}";

        System.out.println("calling the init method with: " + propsJSON);
        pythonHost.callPythonMethod("init", propsJSON);

        String sentenceWithName = pythonHost.callPythonMethod("anonymize",
                "this is my name John Doe", "en").toString();
        String sentenceWithAddress = pythonHost.callPythonMethod("anonymize",
                "this is my address 1 Main Street, Los Angeles CA 09098", "en").toString();

        Assertions.assertTrue(sentenceWithName.contains("<PERSON>"));
        Assertions.assertTrue(sentenceWithAddress.contains("<LOCATION>"));

        // remove temp venv folder
        TestUtils.deleteDirectory(new File(tmpDir));
    }
}