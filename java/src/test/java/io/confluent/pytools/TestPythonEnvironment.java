package io.confluent.pytools;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

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

}