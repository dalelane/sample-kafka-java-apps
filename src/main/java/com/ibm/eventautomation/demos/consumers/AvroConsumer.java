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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.ibm.eventautomation.demos.utils.Utils;

/**
 * Consumes binary-encoded Avro message data from a Kafka topic, and
 * prints it to stdout.
 *
 * The source code needs to be modified to align with the Avro schema
 * used to serialize the message data.
 */
public class AvroConsumer {

    /** Location of the Avro schema that will be used to deserialize the message value. */
    private static final Path SCHEMA = Paths.get("./testdata/avro/schema.avsc");

    /** Config to use for the connection to the Kafka cluster. */
    private static final Path CLIENT_CONFIG = Paths.get("./testdata/consumer.properties");


    /** Placeholder method that prints the message contents to stdout. */
    private static void print(GenericRecord value) {
        // TODO modify this to match the message schema
        System.out.println("id       : " + value.get("id"));
        System.out.println("name     : " + value.get("name"));
        System.out.println("price    : " + value.get("price"));
        System.out.println("quantity : " + value.get("quantity"));
        System.out.println("");
    }

    private static void run() throws IOException {

        // prepare the deserializer for the Avro schema that will be used
        AvroResources avro = parseSchema(SCHEMA);

        // read Kafka client configuration from the properties file
        Properties kafkaConfig = Utils.readProperties(CLIENT_CONFIG);
        kafkaConfig.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        kafkaConfig.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());

        // connect to the Kafka topic
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(kafkaConfig)) {
            consumer.subscribe(Collections.singletonList(kafkaConfig.getProperty("topic")));

            // consume from the topic until the app is killed
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(30));

                for (ConsumerRecord<String, byte[]> record : records) {
                    // for each record, deserialize and print it
                    GenericRecord value = avro.deserialize(record.value());
                    print(value);
                }
            }
        }
    }



    private static AvroResources parseSchema(Path avroSchema) throws IOException {
        AvroResources resources = new AvroResources();
        resources.schema = new Schema.Parser().parse(avroSchema.toFile());
        resources.reader = new GenericDatumReader<GenericRecord>(resources.schema);
        return resources;
    }

    private static final class AvroResources {
        Schema schema;
        GenericDatumReader<GenericRecord> reader;
        BinaryDecoder decoder = null;

        private GenericRecord deserialize(byte[] data) throws IOException {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
                decoder = DecoderFactory.get().binaryDecoder(data, decoder);
                return reader.read(null, decoder);
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
