package io.confluent.pytools;

import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

public class PythonEnvironment {

    private PythonInterpreterConfig config;
    private PythonInterpreter interpreter;
    public PythonEnvironment(String pythonExecutablePath, String[] paths) {
        config = PythonInterpreterConfig.newBuilder()
                .setPythonExec(pythonExecutablePath)
                .setExcType(PythonInterpreterConfig.ExecType.MULTI_THREAD)
                .addPythonPaths(paths)
                .build();

        interpreter = new PythonInterpreter(config);
    }

    private void createVirtualEnvironment(String pythonExecutable, String venvPath) {

    }

    public void executePythonStatement(String pythonStatement) {
        interpreter.exec(pythonStatement);
    }

    public Object callPythonMethod(String methodName, Object... args) {
        return interpreter.invoke(methodName, args);
    }
}
