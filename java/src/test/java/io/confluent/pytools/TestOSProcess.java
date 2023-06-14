package io.confluent.pytools;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class TestOSProcess {
    @Test
    void exec() {
        String output;
        HashMap<String, String> envVars = new HashMap<String, String>();

        output = OperatingSystemProcess.execute(new String[]{"whereis", "python3"}, envVars);

        envVars.put("BLAH", "blaaaah");
        output = OperatingSystemProcess.execute(new String[]{"printenv", "BLAH"}, envVars);
        assertEquals(output, "blaaaah");
    }

    @Test
    void failure() {
        HashMap<String, String> envVars = new HashMap<String, String>();

        assertThrows(IOException.class, () -> {
            OperatingSystemProcess.execute(new String[]{"blah", "blah"}, envVars);
        });

        assertThrows(IOException.class, () -> {
            OperatingSystemProcess.execute(new String[]{"find", "-name", "doesnotexist.file"}, envVars);
        });
    }
}