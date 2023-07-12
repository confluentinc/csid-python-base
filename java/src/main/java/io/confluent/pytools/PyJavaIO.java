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
    static Struct jsonToStruct(Schema itemSchema, String jsonString) {
        Struct rec = new Struct(itemSchema);
        JSONObject jo = new JSONObject(jsonString);
        for (Field field: rec.schema().fields()) {
            rec.put(field.name(), typedParse(field.schema(), jo.get(field.name())));
        }
        return rec;
    }

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
    static Schema getSchemaFromJavaClassName(String className) {
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

    // TODO use Guava's checkedCast()
    static Object castFromDataType(Object data, Schema requestedType) {
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
