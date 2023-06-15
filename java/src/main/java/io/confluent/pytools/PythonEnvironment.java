package io.confluent.pytools;

import lombok.SneakyThrows;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

public class PythonEnvironment {

    //private final PythonInterpreterConfig config;
    private final PythonInterpreter interpreter;
    public PythonEnvironment(String pythonExecutablePath, String[] paths) {
        PythonInterpreterConfig config = PythonInterpreterConfig.newBuilder()
                .setPythonExec(pythonExecutablePath)
                .setExcType(PythonInterpreterConfig.ExecType.MULTI_THREAD)
                .addPythonPaths(paths)
                .build();

        interpreter = new PythonInterpreter(config);
    }
    public void executePythonStatement(String pythonStatement) {
        interpreter.exec(pythonStatement);
    }

    public Object callPythonMethod(String methodName, Object... args) {
        return interpreter.invoke(methodName, args);
    }

    /**
     * builds a complete python virtual environment
     * and returns a PythonEnvironment object that can be used to run Python code
     *
     * @param pipRequirements: required,
     * @param workingDirectory: required, where to install the venv and the files
     *                        if venvName = "venv1" and workingDirectory = "/app/",
     *                        the python environment will be created in "/app/venv1/"
     * @param pythonExecutablePath: optional, the python exe to used (default = default python3 exe of the system)
     * @param venvName: optional, the virtual environment name to use (default = "venv-<uuid>")
     */
    @SneakyThrows
    public static void build(String[] pipRequirements, Path workingDirectory,
                             Path pythonExecutablePath, String venvName) {
        ArrayList<String> paths = new ArrayList<String>();

        Path finalPythonExecutablePath = pythonExecutablePath;
        if (finalPythonExecutablePath == null) {
            finalPythonExecutablePath = defaultPythonExecutablePath();
        }

        // create venv
        String finalVenvName = venvName;
        if (finalVenvName == null) {
            finalVenvName = "venv-" + UUID.randomUUID().toString();
        }
        Path venvPath = Paths.get(workingDirectory.toString(), venvName);

        Path venvPythonExecutablePath = createVirtualEnvironment(finalPythonExecutablePath, venvPath);
        Path defaultSitePackages = Paths.get(getSitePackages(venvPythonExecutablePath.toString()));

        paths.add(defaultSitePackages.toString());
        // install pip reqs
        // returns new PythonEnvironment with the proper paths
    }

    private static Path createVirtualEnvironment(Path pythonExecutable, Path venvPath) {
        OperatingSystemProcess.execute(new String[]{pythonExecutable.toString(), "-m", "venv", venvPath.toString()});
        return Paths.get(venvPath.toString(), "bin", "python");
    }

    @SneakyThrows
    public static Path defaultPythonExecutablePath() {
        String cmdOutput = OperatingSystemProcess.execute(new String[]{"whereis", "python3"});
        String[] items = cmdOutput.split(" ");
        if (items.length < 2) {
            throw new IOException("No default python3 instance found");
        }
        return Paths.get(items[1]);
    }

    private static final String GET_CURRENT_SITE_PACKAGES_PATH_SCRIPT = "import sysconfig; print(sysconfig.get_paths()[\"purelib\"])";
    public static String getSitePackages(String pythonExecutablePath) {
        return OperatingSystemProcess.execute(new String[]{
                pythonExecutablePath, "-c", GET_CURRENT_SITE_PACKAGES_PATH_SCRIPT});
    }

}
