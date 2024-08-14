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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.ibm.eventautomation.demos.utils.Utils;

/**
 * Produces 10,000 randomly generated 128-byte arrays, each as
 *  a separate message.
 *
 *  Callbacks are used to count the number of messages that
 *  fail to send, and the app prints a message on completion
 *  to confirm how many messages were successfully sent.
 */
public class RandomBytesProducer {

    /** Config to use for the connection to the Kafka cluster. */
    private static final Path CLIENT_CONFIG = Paths.get("./testdata/producer.properties");

    /** Number of messages containing random data to try to send. */
    private static final int NUM_MESSAGES_TO_ATTEMPT = 10_000;


    public static void run() throws IOException {

        // read Kafka client configuration from the properties file
        Properties kafkaConfig = Utils.readProperties(CLIENT_CONFIG);
        kafkaConfig.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        kafkaConfig.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());

        // prepare the producer to send messages
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(kafkaConfig)) {

            final AtomicLong failedToSend = new AtomicLong(0);
            long attemptedToSend = 0;

            long startMilliseconds = System.currentTimeMillis();

            while (attemptedToSend < NUM_MESSAGES_TO_ATTEMPT) {
                // send message containing random data
                producer.send(
                    new ProducerRecord<String,byte[]>(
                        kafkaConfig.getProperty("topic"),
                        Utils.randomData()),
                    (meta, exc) -> {
                        if (exc != null) {
                            failedToSend.incrementAndGet();
                        }
                    }
                );

                attemptedToSend += 1;
            }

            // wait for messages to finish sending
            producer.flush();

            // compute time taken to send
            long nowMilliseconds = System.currentTimeMillis();
            System.out.format("%d messages successfully produced in %d seconds. %n",
                              attemptedToSend - failedToSend.get(),
                              Math.round((nowMilliseconds - startMilliseconds) / 1000));
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
