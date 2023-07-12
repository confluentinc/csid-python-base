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
import java.util.Objects;

import static java.util.Map.entry;

public class PyJavaIO {
    final static String KEY = "key";
    final static String VALUE = "value";
    final static String DATA = "data";
    final static String TYPE = "type";
    //

    static Schema getSchemaFromDataType(Object data) {
        if (data instanceof java.lang.Long) {
            return Schema.INT64_SCHEMA;
        } else if (data instanceof java.lang.Double) {
            return Schema.FLOAT64_SCHEMA;
        } else if (data instanceof java.lang.Boolean) {
            return Schema.BOOLEAN_SCHEMA;
        } else if (data instanceof java.lang.Byte) {
            return Schema.BYTES_SCHEMA;
        }
        return Schema.STRING_SCHEMA;
    }

    static Schema getSchemaFromPollResult(HashMap<String, Object> keyOrValue, String scriptName) {
        Schema schema;
        try {
            String structName = scriptName + ".key";
            if (keyOrValue.get(TYPE) == null) {
                schema = getSchemaFromDataType(keyOrValue.get(VALUE));
            } else {
                schema = schemaFromTypeName(keyOrValue.get(TYPE).toString(), structName);
            }
            if (schema.type() == Schema.Type.STRUCT) {
                schema = buildSchemaFromObject(structName, (HashMap<String, Object>)keyOrValue.get(DATA));
            }
        } catch (NullPointerException e) {
            schema = null;
        }
        return schema;
    }

    static Object getDataFromPollResult(HashMap<String, Object> keyOrValue, Schema dataSchema) {
        Object data;
        try {
            if (dataSchema.type() == Schema.Type.STRUCT) {
                data = populateFieldsDataFromObject(dataSchema, (HashMap<String, Object>)keyOrValue.get(DATA));
            } else {
                data = castFromTypeHint(keyOrValue.get(DATA), dataSchema);
            }
        } catch (NullPointerException e) {
            data = null;
        }
        return data;
    }

    static SourceRecord pollResultToSourceRecord(HashMap<String, HashMap<String, Object>> pollResult,
                                                 Map<String, Object> sourcePartition, Map<String, Object> sourceOffset,
                                                 String topic,
                                                 ConnectHeaders headers,
                                                 String scriptName) {

        Schema keySchema = getSchemaFromPollResult(pollResult.get(KEY), scriptName);
        Object key = getDataFromPollResult(pollResult.get(KEY), keySchema);

        Schema valueSchema = getSchemaFromPollResult(pollResult.get(VALUE), scriptName);
        Object value = getDataFromPollResult(pollResult.get(VALUE), valueSchema);

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


    /*
    [Z = boolean
    [B = byte
    [S = short
    [I = int
    [J = long
    [F = float
    [D = double
    [C = char
    [L = any non-primitives(Object)
     */
    static Schema javaClassToSchema(String className) {
        Map<String, Schema> classToSchemaMap = Map.ofEntries(
                entry("java.lang.String", Schema.STRING_SCHEMA),
                entry("java.lang.Short", Schema.INT16_SCHEMA),
                entry("java.lang.Integer", Schema.INT32_SCHEMA),
                entry("java.lang.Long", Schema.INT64_SCHEMA),
                entry("java.lang.Float", Schema.FLOAT32_SCHEMA),
                entry("java.lang.Double", Schema.FLOAT64_SCHEMA),
                entry("java.lang.Boolean", Schema.BOOLEAN_SCHEMA),
                entry("[B", Schema.BYTES_SCHEMA),
                entry("java.lang.Byte", Schema.BYTES_SCHEMA) // ???
        );
        Schema correspondingSchema = classToSchemaMap.get(className);
        return Objects.requireNonNullElse(correspondingSchema, Schema.STRING_SCHEMA);
    }

    // TODO support for nested types
    static void populateSchemaFieldsFromObject(SchemaBuilder builder, HashMap<String, Object> objectMap) {
        for (Map.Entry<String, Object> entry : objectMap.entrySet()) {
            Schema fieldSchema = javaClassToSchema(
                    entry.getValue().getClass().toString().replaceFirst("class ", ""));
            String fieldName = entry.getKey();
            builder.field(fieldName, fieldSchema);
        }
    }

    // pemja casts python ints into longs (INT64) and floats into doubles (FLOAT64)
    // if the type requested in the dict returned by the entry point is different, we do a cast
    // however we only do from ints to (smaller) ints and floats to (smaller) floats

    // TODO use Guava's checkedCast()
    static Object castFromTypeHint(Object data, Schema requestedType) {
        if (data instanceof java.lang.Long) {
            if (requestedType == Schema.INT32_SCHEMA) {
                return ((Long) data).intValue();
            }
            if (requestedType == Schema.INT16_SCHEMA) {
                return ((Long) data).shortValue();
            }
        } else if (data instanceof java.lang.Double) {
            if (requestedType == Schema.FLOAT32_SCHEMA) {
                return ((Double)data).floatValue();
            }
        }
        return data;
    }

    static Struct populateFieldsDataFromObject(Schema structSchema, HashMap<String, Object> map) {
        Struct record = new Struct(structSchema);
        int i = 0;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Schema requestedType = structSchema.fields().get(i++).schema();
            Object data = castFromTypeHint(entry.getValue(), requestedType);
            record.put(entry.getKey(), data);
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
                return Short.parseShort(itemData.toString());
            case INT32:
                return Integer.parseInt(itemData.toString());
            case INT64:
                return Long.parseLong(itemData.toString());
            case FLOAT32:
                return Float.parseFloat(itemData.toString());
            case FLOAT64:
                return Double.parseDouble(itemData.toString());
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
