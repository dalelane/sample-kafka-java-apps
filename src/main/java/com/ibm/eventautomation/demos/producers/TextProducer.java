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
package com.ibm.eventautomation.demos.producers;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.ibm.eventautomation.demos.utils.Utils;

/**
 * Reads the contents of files, and produces the contents of each file
 *  to a Kafka topic as a string.
 *
 * It continues until it has done this for all of the txt files in the
 *  specified folder.
 */
public class TextProducer {

    /** Location of the folder containing the text files to produce to Kafka. */
    private static final Path TEST_DATA_FOLDER = Paths.get("./testdata/text");

    /** Config to use for the connection to the Kafka cluster. */
    private static final Path CLIENT_CONFIG = Paths.get("./testdata/producer.properties");


    public static void run() throws IOException {

        // read Kafka client configuration from the properties file
        Properties kafkaConfig = Utils.readProperties(CLIENT_CONFIG);
        kafkaConfig.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        kafkaConfig.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());

        // prepare the producer to send messages
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaConfig)) {

            // for each file in the folder...
            for (File file : Utils.getFiles(TEST_DATA_FOLDER, ".txt")) {
                // send the contents to Kafka as a string
                producer.send(
                    new ProducerRecord<String, String>(
                            kafkaConfig.getProperty("topic"),
                            Utils.readFileAsString(file))
                );
            }

            producer.flush();
        }
    }



    public static void main(String[] args) {
        try {
            run();
        }
        catch (IOException exc) {
            exc.printStackTrace();
        }
    }
}
