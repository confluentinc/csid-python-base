package io.confluent.pytools;

import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TestPythonEnvironment {
    @Test
    void basic() {
        PythonEnvironment pyEnv = new PythonEnvironment(
                "/Users/laurent/Virtualenvs/pemja/bin/python3.10",
                new String[]{"/Users/laurent/Virtualenvs/pemja/lib/python3.10/site-packages"});
        assertNotNull(pyEnv);
        pyEnv.executePythonStatement("import algorithms.strings as s");

        Object res = pyEnv.callPythonMethod("s.decode_string", "3[a]2[bc]");
        assertEquals(res, "aaabcbc");
        //pyEnv.executePythonStatement("print(s.decode_string(\"3[a]2[bc]\"))");
    }

    @Test
    void defaultPython() {
        Path myPython = PythonEnvironment.defaultPythonExecutablePath();
        assertEquals(myPython.toString(), "/usr/bin/python3");
    }

    public static void deleteDirectory(File directory) {
        // if the file is directory or not
        if (directory.isDirectory()) {
            File[] files = directory.listFiles();

            // if the directory contains any file
            if (files != null) {
                for (File file : files) {
                    // recursive call if the subdirectory is non-empty
                    deleteDirectory(file);
                }
            }
        }

        if (directory.delete()) {
            System.out.println(directory + " is deleted");
        }
        else {
            System.out.println("Directory not deleted");
        }
    }

    @SneakyThrows
    @Test
    void venvInstall() {
        // use volatile java temp dir instead of /tmp/
        String tmpDir = Files.createTempDirectory(null).toFile().getAbsolutePath();

        String defaultPythonPath = PythonEnvironment.defaultPythonExecutablePath().toString();

        PythonEnvironment pyEnv = PythonEnvironment.build(new String[]{"find-libpython==0.3.0", "pemja==0.3.0"}, Paths.get(tmpDir),
                Paths.get(defaultPythonPath), null, null);

        String envPython = pyEnv.getPythonExePath();
        Assertions.assertTrue(Files.exists(Paths.get(envPython)));

        String sitePackages = PythonEnvironment.getSitePackages(envPython);

        Path pemjaLib = Paths.get(sitePackages, "pemja");
        Assertions.assertTrue(Files.exists(pemjaLib));

        // remove temp venv folder
        deleteDirectory(new File(tmpDir));
    }

}