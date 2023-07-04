package io.confluent.pytools.testutils;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class User {
    @JsonProperty
    public String firstName;

    @JsonProperty
    public String lastName;

    @JsonProperty
    public int age;

    public User(String firstName, String lastName, int age) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
    }
}

/*
  Schema schema = SchemaBuilder.struct().name(NAME)
          .field("name", Schema.STRING_SCHEMA)
          .field("age", Schema.INT_SCHEMA)
          .field("admin", new SchemaBuilder.boolean().defaultValue(false).build())
        .build();

        Struct struct = new Struct(schema)
        .put("name", "Barbara Liskov")
        .put("age", 75)
        .build();*/
