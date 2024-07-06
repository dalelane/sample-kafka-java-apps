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
package com.ibm.eventautomation.demos.consumers;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.ibm.eventautomation.demos.utils.Utils;

/**
 * Consumes Kafka messages as strings, and prints them to stdout.
 */
public class TextConsumer {

    /** Config to use for the connection to the Kafka cluster. */
    private static final Path CLIENT_CONFIG = Paths.get("./testdata/consumer.properties");


    /** Placeholder method that prints the message contents to stdout. */
    private static void print(String message) {
        System.out.println(message);
    }

    private static void run() throws IOException {

        // read Kafka client configuration from the properties file
        Properties kafkaConfig = Utils.readProperties(CLIENT_CONFIG);
        kafkaConfig.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        kafkaConfig.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());

        // connect to the Kafka topic
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConfig)) {
            consumer.subscribe(Collections.singletonList(kafkaConfig.getProperty("topic")));

            // consume from the topic until the app is killed
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(30));

                for (ConsumerRecord<String, String> record : records) {
                    // for each record, print it
                    String value = record.value();
                    print(value);
                }
            }
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
