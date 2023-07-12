package io.confluent.pytools;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.Map;

public class PythonPollResult {

    private Schema keySchema;
    private Object key;
    private Schema valueSchema;
    private Object value;

    final static String KEY = "key";
    final static String VALUE = "value";

    // TODO support for nested types
    private static void populateSchemaFieldsFromObject(SchemaBuilder builder, HashMap<String, Object> objectMap) {
        for (Map.Entry<String, Object> entry : objectMap.entrySet()) {
            Schema fieldSchema = PyJavaIO.getSchemaFromJavaClassName(
                    entry.getValue().getClass().toString().replaceFirst("class ", ""));
            String fieldName = entry.getKey();
            builder.field(fieldName, fieldSchema);
        }
    }

    private static Schema buildSchemaFromObject(String structName, HashMap<String, Object> objectMap) {
        SchemaBuilder builder = new SchemaBuilder(Schema.Type.STRUCT);
        builder.name(structName);
        populateSchemaFieldsFromObject(builder, objectMap);
        return builder.build();
    }

    private static Schema getSchemaFromPollResultAsMap(HashMap<String, Object> keyOrValue, String scriptName) {
        String structName = scriptName + ".key";
        return buildSchemaFromObject(structName, keyOrValue);
    }

    private Schema getSchemaFromPollResult(Object keyOrValue, String scriptName) {
        if (keyOrValue instanceof HashMap) {
            return getSchemaFromPollResultAsMap((HashMap<String, Object>)keyOrValue, scriptName);
        }
        return PyJavaIO.getSchemaFromDataType(keyOrValue);
    }

    private static Struct populateFieldsDataFromObject(Schema structSchema, HashMap<String, Object> map) {
        Struct record = new Struct(structSchema);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            record.put(entry.getKey(), entry.getValue());
        }
        return record;
    }

    private static Object getDataFromPollResultAsMap(HashMap<String, Object> keyOrValue, Schema dataSchema) {
        Object data;
        try {
            if (dataSchema.type() == Schema.Type.STRUCT) {
                data = populateFieldsDataFromObject(dataSchema, keyOrValue);
            } else {
                data = PyJavaIO.castFromDataType(keyOrValue, dataSchema);
            }
        } catch (NullPointerException e) {
            data = null;
        }
        return data;
    }

    private static Object getDataFromPollResult(Object keyOrValue, Schema dataSchema) {
        if (keyOrValue instanceof HashMap) {
            return getDataFromPollResultAsMap((HashMap<String, Object>)keyOrValue, dataSchema);
        }

        try {
            if (dataSchema.type() == Schema.Type.STRUCT) {
                return populateFieldsDataFromObject(dataSchema, (HashMap<String, Object>)keyOrValue);
            } else {
                return keyOrValue;
            }
        } catch (NullPointerException e) {
            return null;
        }
    }

    public PythonPollResult(HashMap<String, HashMap<String, Object>> rawResult,
                            String scriptName) {
        keySchema = getSchemaFromPollResult(rawResult.get(KEY), scriptName);
        key = getDataFromPollResult(rawResult.get(KEY), keySchema);

        valueSchema = getSchemaFromPollResult(rawResult.get(VALUE), scriptName);
        value = getDataFromPollResult(rawResult.get(VALUE), valueSchema);
    }

    public SourceRecord toSourceRecord(Map<String, Object> sourcePartition,
                                       Map<String, Object> sourceOffset,
                                       String topic,
                                       ConnectHeaders headers) {
        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                topic,
                null,
                keySchema,
                key,
                valueSchema,
                value,
                null,
                headers
        );
    }
}
