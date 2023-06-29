package io.confluent.pytools;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;

import java.util.HashMap;

public class PyJavaIO {

    static Object matchingParse(Schema itemSchema, Object itemData) {
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
            case STRING:
            default:
                return itemData;
        }
    }

}
