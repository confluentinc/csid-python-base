package io.confluent.pytools;

import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class PyJavaIO {
    final static String KEY = "key";
    final static String VALUE = "value";
    final static String TYPE = "type";
    //
    static SourceRecord pollResultToSourceRecord(HashMap<String, HashMap<String, Object>> pollResult,
                                                 Map<String, Object> sourcePartition, Map<String, Object> sourceOffset,
                                                 String topic,
                                                 ConnectHeaders headers,
                                                 String scriptName) {

        // TODO make generic method and call for key then value
        Schema keySchema;
        Object key;
        try {
            String structName = scriptName + ".key";
            keySchema = schemaFromTypeName(pollResult.get(KEY).get(TYPE).toString(), structName);
            if (keySchema.type() == Schema.Type.STRUCT) {
                HashMap<String, Object> object = (HashMap<String, Object>)pollResult.get(KEY).get(VALUE);
                keySchema = buildSchemaFromObject(structName, object);
                key = populateFieldsDataFromObject(keySchema, object);
            } else {
                key = pollResult.get(KEY).get(VALUE);
            }
        } catch (NullPointerException e) {
            keySchema = null;
            key = null;
        }

        Schema valueSchema;
        Object value;
        try {
            String structName = scriptName + ".value";
            valueSchema = schemaFromTypeName(pollResult.get(VALUE).get(TYPE).toString(), structName);
            if (valueSchema.type() == Schema.Type.STRUCT) {
                HashMap<String, Object> object = (HashMap<String, Object>)pollResult.get(VALUE).get(VALUE);
                valueSchema = buildSchemaFromObject(structName, object);
                value = populateFieldsDataFromObject(valueSchema, object);
            } else {
                value = pollResult.get(VALUE).get(VALUE);
            }
        } catch (NullPointerException e) {
            valueSchema = null;
            value = null;
        }

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
    static Struct jsonToStruct(Schema itemSchema, String jsonString) {
        Struct rec = new Struct(itemSchema);
        JSONObject jo = new JSONObject(jsonString);
        for (Field field: rec.schema().fields()) {
            rec.put(field.name(), typedParse(field.schema(), jo.get(field.name())));
        }
        return rec;
    }

    static Schema buildSchemaFromObject(String structName, HashMap<String, Object> objectMap) {
        SchemaBuilder builder = new SchemaBuilder(Schema.Type.STRUCT);
        builder.name(structName);
        populateSchemaFieldsFromObject(builder, objectMap);
        return builder.build();
    }

    static Schema javaClassToSchema(String className) {
        if (className.contains("Long")) {
            return schemaFromTypeName("INT64", null);
        } else if (className.contains("String")) {
            return schemaFromTypeName("STRING", null);
        }
        return schemaFromTypeName("STRING", null);
    }

    // TODO no support for nested types yet
    static void populateSchemaFieldsFromObject(SchemaBuilder builder, HashMap<String, Object> objectMap) {
        for (Map.Entry<String, Object> entry : objectMap.entrySet()) {
            Schema fieldSchema = javaClassToSchema(entry.getValue().getClass().toString());
            String fieldName = entry.getKey();
            builder.field(fieldName, fieldSchema);
        }
    }

    static Struct populateFieldsDataFromObject(Schema structSchema, HashMap<String, Object> map) {
        Struct record = new Struct(structSchema);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            record.put(entry.getKey(), entry.getValue());
        }
        return record;
    }

    static Schema schemaFromTypeName(String typeName, String structName) {
        switch (typeName) {
            case "INT8":
                return Schema.INT8_SCHEMA;
            case "INT16":
                return Schema.INT16_SCHEMA;
            case "INT32":
                return Schema.INT32_SCHEMA;
            case "INT64":
                return Schema.INT64_SCHEMA;
            case "FLOAT32":
                return Schema.FLOAT32_SCHEMA;
            case "FLOAT64":
                return Schema.FLOAT64_SCHEMA;
            case "BOOLEAN":
                return Schema.BOOLEAN_SCHEMA;
            case "STRUCT":
                return SchemaBuilder.struct().name(structName).build();
            case "STRING":
            default:
                return Schema.STRING_SCHEMA;
        }
    }

    static Object typedParse(Schema itemSchema, Object itemData) {
        switch (itemSchema.type()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                return Integer.parseInt(itemData.toString());
            case FLOAT32:
            case FLOAT64:
                return Float.parseFloat(itemData.toString());
            case BOOLEAN:
                return Boolean.parseBoolean(itemData.toString());
            case STRUCT:
                return jsonToStruct(itemSchema, itemData.toString());
            case STRING:
            default:
                return itemData;
        }
    }

    static String structToJSON(Object payload) {
        String strVal = payload.toString();
        // quick and dirty Struct to JSON =
        // 1. remove Struct prefix
        // 2. put everything in quotes
        // 3. replace = with :
        strVal = strVal.substring(7, strVal.length()-1);
        ArrayList<String> resultItems = new ArrayList<>();
        String[] items = strVal.split(",");
        for (String item: items) {
            String[] keyVals = item.split("=");
            resultItems.add("\"" + keyVals[0] + "\":\"" + keyVals[1] + "\""); // TODO check array boundaries
        }
        String itemsJoined = String.join(",", resultItems);
        return "{" + itemsJoined + "}";
    }

    @SneakyThrows
    static Object payloadTyped(Object payload, String schema) {
        // if the schema is a Struct, we pass the payload as a JSON String
        // otherwise if it's a basic type, we pass the toString()
        if (schema.contains("STRUCT")) {
            return structToJSON(payload);
        } else {
            return payload;
        }
    }

    static String normalizedTypeName(Schema schema) {
        return schema.toString()
                .replace("}", "")
                .replace("{", "")
                .replaceFirst("record:", "")
                .replaceFirst("Schema", "")
                .replaceFirst("STRUCT", "JSON");
    }

}
