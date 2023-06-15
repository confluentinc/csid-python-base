package io.confluent.pytools;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;

public class OperatingSystemProcess {

    @SneakyThrows
    public static String execute(String[] commands) {
        HashMap<String, String> envVars = new HashMap<String, String>();
        return execute(commands, envVars);
    }

    @SneakyThrows
    public static String execute(String[] commands, Map<String, String> environmentVariables) {

        ProcessBuilder pb = new ProcessBuilder(commands);
        pb.environment().putAll(environmentVariables);
        pb.redirectErrorStream(true);
        Process p = pb.start();

        StringBuilder out = new StringBuilder();
        BufferedReader output = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line;

        while ((line = output.readLine()) != null) {
            out.append(line).append("\n");
        };

        try {
            if (p.waitFor() != 0) {
                throw new IOException(
                        String.format(
                                "Failed to execute the command: %s\noutput: %s",
                                String.join(" ", commands), out
                ));
            }
        } catch (InterruptedException e) {
            // Ignored. The subprocess is dead after "br.readLine()" returns null, so the call of
            // "waitFor" should return intermediately.
        }
        return out.toString().trim();
    }

    @SneakyThrows
    public static String executeWithRetries(String[] commands, Map<String, String> environmentVariables, Integer numRetries) {
        int retries = 0;
        while (true) {
            try {
                return execute(commands, environmentVariables);
            }
            catch (Exception e) {
                retries++;
                if (retries >= numRetries) {
                    throw new IOException(e);
                }
                // log failed exec
            }
        }
    }

}
