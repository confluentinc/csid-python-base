package io.confluent.pytools;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPythonHost {
    @SneakyThrows
    @Test
    void basic() {
        // test files are in resources (requirements.txt + test.py)
        Path requirementsFile = Paths.get("src","test","resources", "requirements.txt");
        Assertions.assertTrue(Files.exists(requirementsFile));

        Path pythonScript = Paths.get("src","test","resources", "test.py");
        Assertions.assertTrue(Files.exists(pythonScript));

        String pythonExecutable = PyUtils.defaultPythonExecutablePath().toString();

        Path scriptsDirectory = Paths.get("src","test","resources");

        PythonHost host = new PythonHost(pythonExecutable, scriptsDirectory.toFile(), "test.hello", ".");
        Assertions.assertNotNull(host);

        host.executePythonStatement("from datetime import datetime as guest"); // should not fail or generate a collision

        Object res = host.callPythonMethod("hello");
        assertEquals(res, "just now");

        res = host.callEntryPoint();
        assertEquals(res, "just now");

        String venvDir = host.venvPath();
        TestUtils.deleteDirectory(new File(venvDir));

        Assertions.assertFalse(Files.exists(Paths.get(venvDir)));
    }

    @SneakyThrows
    @Test
    void advancedEntryPoint() {
        // test files are in resources (requirements.txt + test.py)
        Path requirementsFile = Paths.get("src","test","resources", "requirements.txt");
        Assertions.assertTrue(Files.exists(requirementsFile));

        Path pythonScript = Paths.get("src","test","resources", "adv1", "adv2.py");
        Assertions.assertTrue(Files.exists(pythonScript));

        String pythonExecutable = PyUtils.defaultPythonExecutablePath().toString();

        Path scriptsDirectory = Paths.get("src","test","resources");

        PythonHost host = new PythonHost(pythonExecutable, scriptsDirectory.toFile(), "adv1.adv2.hello", ".");
        Assertions.assertNotNull(host);

        Object res = host.callPythonMethod("hello");
        assertEquals(res, "just now");

        String venvDir = host.venvPath();
        TestUtils.deleteDirectory(new File(venvDir));
    }

    @SneakyThrows
    @Test
    void badEntryPoint() {
        // test files are in resources (requirements.txt + test.py)
        Path requirementsFile = Paths.get("src","test","resources", "requirements.txt");
        Assertions.assertTrue(Files.exists(requirementsFile));

        Path pythonScript = Paths.get("src","test","resources", "test.py");
        Assertions.assertTrue(Files.exists(pythonScript));

        String pythonExecutable = PyUtils.defaultPythonExecutablePath().toString();

        Path scriptsDirectory = Paths.get("src","test","resources");

        Assertions.assertThrows(IOException.class, () -> {
            new PythonHost(pythonExecutable, scriptsDirectory.toFile(), "test.hello2", ".");
        });
    }
}
