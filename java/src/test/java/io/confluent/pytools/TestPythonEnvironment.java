package io.confluent.pytools;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TestPythonEnvironment {
    @SneakyThrows
    @Test
    void venvInstall() {
        // use volatile java temp dir instead of /tmp/
        String tmpDir = Files.createTempDirectory(null).toFile().getAbsolutePath();
        System.out.println("Temp Directory: " + tmpDir);

        String defaultPythonPath = PyUtils.defaultPythonExecutablePath().toString();

        PythonEnvironment pyEnv = PythonEnvironment.build(
                new String[]{"algorithms==0.1.4", "find-libpython==0.3.0", "pemja==0.3.0", "arrow"}, Paths.get(tmpDir),
                Paths.get(defaultPythonPath), null, null, null);

        String envPython = pyEnv.getPythonExePath();
        System.out.println("Virtual environment created with python = " + envPython);

        Assertions.assertTrue(Files.exists(Paths.get(envPython)));

        String sitePackages = PyUtils.getSitePackages(envPython);
        System.out.println("and site packages = " + sitePackages);

        Path pemjaLib = Paths.get(sitePackages, "pemja");
        Assertions.assertTrue(Files.exists(pemjaLib));

        System.out.println("Calling python code using pemja...");
        // test the installed env --> run some python code

        pyEnv.executePythonStatement("from find_libpython import find_libpython");
        pyEnv.executePythonStatement("print(find_libpython())");

        pyEnv.executePythonStatement("import arrow");
        // works but the result (of Object type) is unusable from here
        Object res = pyEnv.callPythonMethod("arrow.utcnow");
        assertNotNull(res);

        pyEnv.executePythonStatement("import algorithms.strings as s");
        res = pyEnv.callPythonMethod("s.decode_string", "3[a]2[bc]");
        assertEquals(res, "aaabcbc");
        System.out.println("OK");

        // remove temp venv folder
        TestUtils.deleteDirectory(new File(tmpDir));
    }

    @SneakyThrows
    @Test
    void venvInstallNamedEnv() {
        // use volatile java temp dir instead of /tmp/
        String tmpDir = Files.createTempDirectory(null).toFile().getAbsolutePath();
        System.out.println("Temp Directory: " + tmpDir);

        String defaultPythonPath = PyUtils.defaultPythonExecutablePath().toString();

        PythonEnvironment pyEnv = PythonEnvironment.build(
                new String[]{"algorithms==0.1.4", "find-libpython==0.3.0", "pemja==0.3.0"}, Paths.get(tmpDir),
                Paths.get(defaultPythonPath), "venv1", null, null);

        String envPython = pyEnv.getPythonExePath();
        System.out.println("Virtual environment created with python = " + envPython);

        Assertions.assertTrue(Files.exists(Paths.get(envPython)));

        String sitePackages = PyUtils.getSitePackages(envPython);
        System.out.println("and site packages = " + sitePackages);

        Path pemjaLib = Paths.get(sitePackages, "pemja");
        Assertions.assertTrue(Files.exists(pemjaLib));

        System.out.print("Calling python code using pemja...");
        // test the installed env --> run some python code
        pyEnv.executePythonStatement("import algorithms.strings as s");
        Object res = pyEnv.callPythonMethod("s.decode_string", "3[a]2[bc]");
        assertEquals(res, "aaabcbc");
        System.out.println("OK");

        // remove temp venv folder
        TestUtils.deleteDirectory(new File(tmpDir));
    }

}