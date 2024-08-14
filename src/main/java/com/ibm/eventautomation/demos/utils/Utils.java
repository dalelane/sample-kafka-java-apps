/**
 * Copyright 2024 IBM Corp. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.ibm.eventautomation.demos.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.common.config.ConfigException;

public class Utils {

    private static final String[] REQUIRED_PROPERTIES = {
        "topic",
        "bootstrap.servers"
    };

    public static Properties readProperties(Path location) throws IOException {
        Properties props = new Properties();
        try (InputStream input = new FileInputStream(location.toFile())) {
            props.load(input);
        }
        for (String prop : REQUIRED_PROPERTIES) {
            if (!props.containsKey(prop) || props.getProperty(prop).isEmpty()) {
                throw new ConfigException("Missing required property " + prop);
            }
        }
        return props;
    }


    public static File[] getFiles(Path location, String extension) {
        return location.toFile().listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(extension);
            }
        });
    }


    public static String readFileAsString(File file) throws IOException {
        return new String(Files.readAllBytes(file.toPath()));
    }


    private static final Random RNG = new Random();
    public static byte[] randomData() {
        byte[] array = new byte[128];
        RNG.nextBytes(array);
        return array;
    }
}
