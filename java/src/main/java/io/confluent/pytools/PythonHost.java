package io.confluent.pytools;

import lombok.SneakyThrows;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static java.nio.file.Files.readAllLines;

public class PythonHost {

    private final String PEMJA_PREFIX = "pemja";
    private final String PEMJA_REQ_VERSION = "pemja==0.3.0";

    private String importStatement;
    private final String guestLibraryAlias;
    private String callableMethod;

    private final PythonEnvironment pythonEnv;

    // TODO move to a static build() pattern?
    public PythonHost(String pythonExecutable, File scriptsDirectory, String entryPoint, String workingDirectory) throws IOException {
        this(pythonExecutable, scriptsDirectory, entryPoint, workingDirectory, null);
    }

    public PythonHost(String pythonExecutable, File scriptsDirectory, String entryPoint, String workingDirectory, String localDependenciesDirectory) throws IOException {
        // explores the working directory to find requirements.txt
        // and build the PythonEnvironment

        // we check that it's a directory
        if (!scriptsDirectory.isDirectory()) {
            String msg = "scriptsDirectory " + scriptsDirectory + " is not a directory.";
            System.out.println("ERROR" + msg);
            throw new IOException(msg);
        }

        File[] files = scriptsDirectory.listFiles();
        if (files == null || files.length == 0) {
            String msg = "scriptsDirectory " + scriptsDirectory + " is empty.";
            System.out.println("ERROR" + msg);
            throw new IOException(msg);
        }

        // any requirements.txt?
        List<String> pipRequirements = new ArrayList<>();
        File[] requirements = scriptsDirectory.listFiles(
                (dir, name) -> name.equalsIgnoreCase("requirements.txt"));
        if (requirements != null && requirements.length > 0) {
            File req = requirements[0];
            pipRequirements = Files.readAllLines(req.toPath(), StandardCharsets.UTF_8);
        }

        ensurePemjaRequirement(pipRequirements);

        System.out.println("requirements before build: " + pipRequirements);

        // search for the file referenced in the entry point
        File[] pythonScripts = scriptsDirectory.listFiles((dir, name) -> name.endsWith(".py"));
        if (pythonScripts == null || pythonScripts.length == 0) {
            String msg = "scriptsDirectory " + scriptsDirectory + " does not contain python scripts.";
            System.out.println("ERROR" + msg);
            throw new IOException(msg);
        }

        // check the entry point and verify we have the file
        buildEntryPoint(entryPoint, scriptsDirectory);

        // build the python environment
        pythonEnv = PythonEnvironment.build(pipRequirements.toArray(new String[0]),
                Paths.get(workingDirectory), Paths.get(pythonExecutable),
                null, localDependenciesDirectory, scriptsDirectory.toString());

        // now that the env is running, we call "import <importStatement>" to be ready to call the function
        guestLibraryAlias = "guest_" + UUID.randomUUID().toString().replace("-", "_");
        pythonEnv.executePythonStatement("import " + importStatement + " as " + guestLibraryAlias);
        //pythonEnv.executePythonStatement("print(dir(" + guestLibraryAlias + "))");

    }

    private void ensurePemjaRequirement(List<String> pipRequirements) {
        for (String line: pipRequirements) {
            if (line.trim().startsWith(PEMJA_PREFIX)) {
                return;
            }
        }
        pipRequirements.add(PEMJA_REQ_VERSION);
    }

    /**
     * The format for entry points is driven by the way pemja works.
     * (and the organization of scripts inside the module)
     * - If the module doesn't have sub-modules, we'll import and call it this way:
     *      pyEnv.executePythonStatement("import arrow");
     *      Object res = pyEnv.callPythonMethod("arrow.utcnow");
     *   So the entry point should be provided as "arrow.utcnow"
     * - If there are sub-modules, we'll import and call it using an alias:
     *      pyEnv.executePythonStatement("import algorithms.strings as s");
     *      res = pyEnv.callPythonMethod("s.decode_string", "3[a]2[bc]");
     *   So the entry point should be provided as "algorithms.strings.decode_string"
     *   and we'll split it at the last dot and use an alias.
     *
     * @param entryPoint: the entry point provided by the user
     * @param scriptsDirectory: the user scripts directory where we'll search for the script
     * referenced in the entry point
     */
    private void buildEntryPoint(String entryPoint, File scriptsDirectory) throws IOException {
        // analyze entry point
        String[] items = entryPoint.split("\\.");
        if (items.length < 2) {
            String msg = "Entry point " + entryPoint + " doesn't have the <script>.<function> format.";
            System.out.println("ERROR" + msg);
            throw new IOException(msg);
        }
        callableMethod = items[items.length-1];

        String[] scriptItems = Arrays.copyOf(items, items.length-1);
        String[] scriptPath = Arrays.copyOf(items, items.length-2);
        String scriptName = items[items.length-2] + ".py";

        importStatement = String.join(".", scriptItems);

        // search for the proper file (using scriptItems)
        Path targetScript = Paths.get(scriptsDirectory.toString(), String.join("/", scriptPath), scriptName);

        // open the file and check that a function with the proper name exists
        List<String> scriptContents;

        try {
            scriptContents = Files.readAllLines(targetScript, StandardCharsets.UTF_8);
        } catch (Exception e) {
            String msg = "Unable to read " + targetScript + ": " + e.toString();
            System.out.println("ERROR" + msg);
            throw new IOException(msg);
        }

        boolean found = false;
        for (String line : scriptContents) {
            if (line.trim().startsWith("def " + callableMethod)) {
                found = true;
                break;
            }
        }
        if (!found) {
            String msg = callableMethod + " function not found in " + targetScript;
            System.out.println("ERROR" + msg);
            throw new IOException(msg);
        }
    }

    public void executePythonStatement(String pythonStatement) {
        pythonEnv.executePythonStatement(pythonStatement);
    }

    public Object callPythonMethod(String methodName, Object... args) {
        return pythonEnv.callPythonMethod(guestLibraryAlias + "." + methodName, args);
    }

    public Object callEntryPoint(Object... args) {
        return pythonEnv.callPythonMethod(guestLibraryAlias + "." + callableMethod, args);
    }

    public String venvPath() {
        return pythonEnv.getVirtualEnvironmentPath();
    }
}
