package io.confluent.pytools.testutils;

import org.junit.jupiter.api.io.TempDir;

import java.io.File;

public class SimpleProducer extends Thread {
    String testTopic;
    CommonTestUtils commonTestUtils;

    public SimpleProducer(String testTopic, CommonTestUtils commonTestUtils) {
        this.testTopic = testTopic;
        this.commonTestUtils = commonTestUtils;
    }

    public void run() {
        System.out.println("Producer thread started");
        String value = "Some string value";
        String key = "1234";
        commonTestUtils.produceSingleEvent(testTopic, key, value);
        System.out.println("Producer thread done");
    }

}
