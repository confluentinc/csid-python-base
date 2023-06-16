package io.confluent.pytools;

import lombok.SneakyThrows;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class PythonHost {

    private String[] requirements;

    private static String importStatement;
    private static String callableMethod;

    private PythonEnvironment pythonEnv;

    public PythonHost(String pythonExecutable, File scriptsDirectory, String entryPoint) throws IOException {
        // explores the corking directory to find requirements.txt
        // and build the PythonEnvironment

        // we check that it's a directory
        if (!scriptsDirectory.isDirectory()) {
            throw new IOException("scriptsDirectory " + scriptsDirectory + " is not a directory.");
        }

        File[] files = scriptsDirectory.listFiles();
        if (files == null) {
            throw new IOException("scriptsDirectory " + scriptsDirectory + " is empty.");
        }

        // any requirements.txt?
        String[] pipRequirements = new String[0];
        File[] requirements = scriptsDirectory.listFiles(
                (dir, name) -> name.equalsIgnoreCase("requirements.txt"));
        if (requirements != null) {
            File req = requirements[0];
            // read it
        }

        // search for the file referenced in the entry point
        File[] pythonScripts = scriptsDirectory.listFiles((dir, name) -> name.endsWith(".py"));
        if (pythonScripts == null) {
            throw new IOException("scriptsDirectory " + scriptsDirectory + " does not contain python scripts.");
        }

        // check the entry point and verify we have the file
        buildEntryPoint(entryPoint, scriptsDirectory);

        // build the python environment
        String workingDirectory = "";
        pythonEnv = PythonEnvironment.build(pipRequirements,
                Paths.get(workingDirectory), Paths.get(pythonExecutable),
                null, null, scriptsDirectory.toString());
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
        String[] items = entryPoint.split(".");
        if (items.length < 2) {
            throw new IOException("Entry point " + entryPoint + " doesn't have the <script>.<function> format.");
        }
        callableMethod = items[items.length-1];
        String[] scriptItems = Arrays.copyOf(items, items.length-1);
        importStatement = String.join(".", scriptItems);

        // search for the proper file (using scriptItems)

        // open the file?
    }
}
