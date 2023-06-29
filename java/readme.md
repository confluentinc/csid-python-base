# CSID Python Tools

## Introduction 

The interface from java to python is done using the pemja library (developed by Alibaba).

https://github.com/alibaba/pemja

How pemja works:
- It starts a thread for the python interpreter in the JVM
- It calls python code using Java to C interface (JNI) and a C python module

## Python Tools (java) Classes

- OperatingSystemProcess - OS process utils/wrapper
- PythonEnvironment - low-level python (virtual) environment
- **PythonHost** - higher-level python host for connectors + SMTs
- PyJavaIO - helpers for parameter checking/passing
- PyUtils - helpers for python defaults on the host system
- **ConnectSmt** - SMT encapsulating the python code
- ConnectConnector - Connector encapsulating the python code

## Python SMT

Main java class = `io.confluent.pytools.ConnectSmt`

Step 1. Add the Python Tools jar to the `CLASSPATH`.

Step 2. Put your python scripts, including an optional `requirements.txt` in a directory (the scripts directory).

Step 3. Add the following properties to the connector you're adding the SMT to:

(full description of the properties later in this document)

```json
"transforms": "myTransform",
"transforms.myTransform.type": "io.confluent.pytools.ConnectSmt",
"transforms.myTransform.scripts.dir": "/app/",
"transforms.myTransform.working.dir": "/temp/venv/",
"transforms.myTransform.entry.point", "transform1.transform",
"transforms.myTransform.init.method", "init",
"transforms.myTransform.private.settings", "{\"conf1\":\"value1\", \"conf2\":\"value2\"}"

```

### How it works

👉 When initializing (during the `configure()` call of the java SMT SDK), the SMT builds a python virtual environment 
and (pip) installs the libraries referenced in the `requirements.txt` file. Then, it calls the `init()` method of the 
python script (if configured). It also installs the `pemja` library in the virtual environment.

For each call of the `apply()` call of the java SMT SDK (the method calling the transformation), it calls the `entry.point` 
python method and passes a dict with the Kafka record (see next section).

### Python script and method signatures

TODO

### Config properties

- `<transform.prefix>.type` must be `io.confluent.pytools.ConnectSmt`
- `<transform.prefix>.scripts.dir`: the directory where the python scripts reside. 
- `<transform.prefix>.working.dir`: optional, the directory where to build the python virtual environment. If not passed, the scripts directory will be used.
- `<transform.prefix>.entry.point`: Python entry point for the transform. See details on python entry points below. 
- `<transform.prefix>.init.method`: optional, method called once by the SMT when it initializes the transform.
- `<transform.prefix>.private.settings`: String passed to the python script. Can be used to put settings in JSON format; eg. `"{\"conf1\":\"value1\", \"conf2\":\"value2\"}"`.
- `<transform.prefix>.offline.installation.dir`: optional, the directory containing wheel/python packages for offline installation of the packages in the virtual environment.

**Note on python entry points**

The format for the python entry points is driven by the way pemja works and the organization of python scripts inside the user modules. 

If the module doesn't have sub-modules, we'll import and call it this way:
```java
pyEnv.executePythonStatement("import <python-module-or-script>");
Object res = pyEnv.callPythonMethod("<python-module-or-script>.<python-method>");
```
So the entry point should be provided as `<python-module-or-script>.<python-method>`. 
For example, if the script is called `transform1.py` and the method is called `transform`, the entry point will be `transform1.transform`.

If there are sub-modules, we'll import and call it using an alias:

```java
pyEnv.executePythonStatement("import algorithms.strings as s1234");
res = pyEnv.callPythonMethod("s1234.decode_string", "3[a]2[bc]");
```

So the entry point should be provided as `algorithms.strings.decode_string` and we'll split it at the last dot and use an alias.



### FAQ

- How to provide packages for offline installation of the python environment? Put the wheel packages in a directory and provide it using `<transform.prefix>.offline.installation.dir`.
- 