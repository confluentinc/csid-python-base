package io.confluent.pytools;

import lombok.SneakyThrows;

import java.io.Console;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class PyUtils {
    @SneakyThrows
    public static Path defaultPythonExecutablePath() {
        String cmdOutput = OperatingSystemProcess.execute(new String[]{"which", "python3"});

        if (cmdOutput.contains("not found")) {
            throw new IOException("No default python3 instance found");
        }
        return Paths.get(cmdOutput);
    }

    private static final String GET_CURRENT_SITE_PACKAGES_PATH_SCRIPT = "import sysconfig; print(sysconfig.get_paths()[\"purelib\"])";
    public static String getSitePackages(String pythonExecutablePath) {
        return OperatingSystemProcess.execute(new String[]{
                pythonExecutablePath, "-c", GET_CURRENT_SITE_PACKAGES_PATH_SCRIPT});
    }
}
