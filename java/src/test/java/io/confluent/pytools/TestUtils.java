package io.confluent.pytools;

import java.io.File;

public class TestUtils {
    public static void deleteDirectory(File directory) {
        // if the file is directory or not
        if (directory.isDirectory()) {
            File[] files = directory.listFiles();

            // if the directory contains any file
            if (files != null) {
                for (File file : files) {
                    // recursive call if the subdirectory is non-empty
                    deleteDirectory(file);
                }
            }
        }

        if (directory.delete()) {
            System.out.println(directory + " is deleted");
        }
        else {
            System.out.println("Directory not deleted");
        }
    }
}
