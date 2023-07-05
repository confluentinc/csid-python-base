package io.confluent.pytools;

import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONObject;

import java.util.ArrayList;

public class PyJavaIO {

    static Struct jsonToStruct(Schema itemSchema, String jsonString) {
        Struct rec = new Struct(itemSchema);
        JSONObject jo = new JSONObject(jsonString);
        for (Field field: rec.schema().fields()) {
            rec.put(field.name(), typedParse(field.schema(), jo.get(field.name())));
        }
        return rec;
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
