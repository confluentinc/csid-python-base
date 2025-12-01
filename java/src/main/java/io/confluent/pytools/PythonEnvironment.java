package io.confluent.pytools;

import lombok.SneakyThrows;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static io.confluent.pytools.OperatingSystemProcess.execute;

public class PythonEnvironment {
    private final PythonInterpreter interpreter;

    private final String pythonExePath;
    private final String virtualEnvironmentPath;
    public PythonEnvironment(String pythonExecutablePath, String[] paths, String venvPath, String pythonHome) {
        PythonInterpreterConfig config = PythonInterpreterConfig.newBuilder()
                .setPythonExec(pythonExecutablePath)
                .setPythonHome(pythonHome)
                .setExcType(PythonInterpreterConfig.ExecType.MULTI_THREAD)  // SUB_INTERPRETER or MULTI_THREAD
                .addPythonPaths(paths)
                .build();

        pythonExePath = pythonExecutablePath;
        virtualEnvironmentPath = venvPath;

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
     * @param pipRequirements            : required,
     * @param workingDirectory           : required, where to install the venv and the files
     *                                   if venvName = "venv1" and workingDirectory = "/app/",
     *                                   the python environment will be created in "/app/venv1/"
     * @param pythonExecutablePath       : optional, the python exe to used (default = default python3 exe of the system)
     * @param venvName                   : optional, the virtual environment name to use (default = "venv-<uuid>")
     * @param localDependenciesDirectory : optional, directory with packages for offline installation
     * @param additionalPath             : optional, directory to be added to the path (used for providing
     *                                   the user scripts directory)
     */
    @SneakyThrows
    public static PythonEnvironment build(String[] pipRequirements, Path workingDirectory,
                                          Path pythonExecutablePath, String venvName, String localDependenciesDirectory,
                                          String additionalPath) {
        HashSet<String> paths = new HashSet<>();

        if (additionalPath != null) {
            paths.add(additionalPath);
        }

        Path finalPythonExecutablePath = pythonExecutablePath;
        if (finalPythonExecutablePath == null) {
            finalPythonExecutablePath = PyUtils.defaultPythonExecutablePath();
        }

        // create venv
        String finalVenvName = venvName;
        if (finalVenvName == null) {
            finalVenvName = "venv-" + UUID.randomUUID();
        }
        Path venvPath = Paths.get(workingDirectory.toString(), finalVenvName);

        Path defaultSitePackages = Paths.get(PyUtils.getSitePackages(finalPythonExecutablePath.toString()));
        paths.add(defaultSitePackages.toString());
        paths.add(defaultSitePackages.toString().replaceFirst("/lib/", "/lib64/"));

        Path venvPythonExecutablePath = createVirtualEnvironment(finalPythonExecutablePath, venvPath);
        Path venvSitePackages = Paths.get(PyUtils.getSitePackages(venvPythonExecutablePath.toString()));
        paths.add(venvSitePackages.toString());
        paths.add(venvSitePackages.toString().replaceFirst("/lib/", "/lib64/"));

        String pythonHome = venvSitePackages.toString().replaceFirst("/site-packages", "");

        // install pip requirements
        pipInstallRequirements(venvPythonExecutablePath.toString(), pipRequirements, localDependenciesDirectory);

        // returns new PythonEnvironment with the proper paths
        System.out.println("new PythonEnvironment: " + venvPythonExecutablePath + ", paths: " + paths +
                ", venPath=" + venvPath+ ", pythonHome=" + pythonHome);
        return new PythonEnvironment(venvPythonExecutablePath.toString(), paths.toArray(String[]::new),
                venvPath.toString(), pythonHome);
    }

    @SneakyThrows
    private static Path createVirtualEnvironment(Path pythonExecutable, Path venvPath) {
        execute(new String[]{pythonExecutable.toString(), "-m", "venv", venvPath.toString()});
        return Paths.get(venvPath.toString(), "bin", "python");
    }

    private static void pipInstallRequirements(String pythonExecutable, String[] requirements,
                                               String localDependenciesDirectory) throws IOException {
        String sitePackagesPath = PyUtils.getSitePackages(pythonExecutable);
        HashMap<String, String> envVars = new HashMap<>();
        envVars.put("PYTHONPATH", sitePackagesPath);

        System.out.println("installing requirements to: " + sitePackagesPath);

        ArrayList<String> pipInstallCommand = new ArrayList<>(List.of(new String[]{pythonExecutable, "-m", "pip", "install"}));
        pipInstallCommand.addAll(List.of(requirements));
        if (localDependenciesDirectory != null && localDependenciesDirectory.trim().length() > 0) {
            pipInstallCommand.addAll(List.of("--find-links", localDependenciesDirectory));
        }

        execute(pipInstallCommand.toArray(String[]::new), envVars);
    }

    public String getPythonExePath() {
        return pythonExePath;
    }

    public String getVirtualEnvironmentPath() {
        return virtualEnvironmentPath;
    }
}
