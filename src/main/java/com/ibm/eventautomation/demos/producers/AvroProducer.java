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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.ibm.eventautomation.demos.utils.Utils;

/**
 * Produces binary-encoded Avro message data to a Kafka topic.
 *
 * The source code needs to be modified to align with the Avro schema
 * used to serialize the message data.
 */
public class AvroProducer {

    /** Location of the Avro schema that will be used to serialize the message value. */
    private static final Path SCHEMA = Paths.get("./testdata/avro/schema.avsc");

    /** Config to use for the connection to the Kafka cluster. */
    private static final Path CLIENT_CONFIG = Paths.get("./testdata/producer.properties");

    // -----------------------------------------------
    // Sample messages
    //  if modifying the schema, these will need to be
    //  updated to match
    // -----------------------------------------------
    // TODO modify these to match the message schema
    private static byte[] createFirstMessage(AvroResources avro) throws IOException {
        GenericRecord avroRecord = avro.builder
            .set("id",       "4efb1831-d7fc-4665-8ff8-bce4e777b37f")
            .set("name",     "Test Item")
            .set("price",    47.99)
            .set("quantity", 2)
            .build();
        return avro.serialize(avroRecord);
    }
    private static byte[] createSecondMessage(AvroResources avro) throws IOException {
        GenericRecord avroRecord = avro.builder
            .set("id",       "d24c096c-10d4-414a-8ee2-9282d526c1d3")
            .set("name",     "Something else")
            .set("price",    3.99)
            .set("quantity", 8)
            .build();
        return avro.serialize(avroRecord);
    }
    private static byte[] createThirdMessage(AvroResources avro) throws IOException {
        GenericRecord avroRecord = avro.builder
            .set("id",       "37cdd94e-cbc3-4294-a55b-21309c35fcec")
            .set("name",     "Large item")
            .set("price",    289.99)
            .set("quantity", 1)
            .build();
        return avro.serialize(avroRecord);
    }



    public static void run() throws IOException {

        // prepare the serializer for the Avro schema that will be used
        AvroResources avro = parseSchema(SCHEMA);

        // read Kafka client configuration from the properties file
        Properties kafkaConfig = Utils.readProperties(CLIENT_CONFIG);
        kafkaConfig.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        kafkaConfig.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());

        // prepare the producer to send messages
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(kafkaConfig)) {

            // create and send three test messages
            producer.send(
                new ProducerRecord<String, byte[]>(
                        kafkaConfig.getProperty("topic"),
                        createFirstMessage(avro))
            );
            producer.send(
                new ProducerRecord<String, byte[]>(
                        kafkaConfig.getProperty("topic"),
                        createSecondMessage(avro))
            );
            producer.send(
                new ProducerRecord<String, byte[]>(
                        kafkaConfig.getProperty("topic"),
                        createThirdMessage(avro))
            );

            // wait for messages to finish sending
            producer.flush();
        }
    }



    private static AvroResources parseSchema(Path avroSchema) throws IOException {
        AvroResources resources = new AvroResources();
        resources.schema = new Schema.Parser().parse(avroSchema.toFile());
        resources.builder = new GenericRecordBuilder(resources.schema);
        resources.writer = new GenericDatumWriter<>(resources.schema);
        return resources;
    }

    private static final class AvroResources {
        Schema schema;
        GenericRecordBuilder builder;
        DatumWriter<GenericRecord> writer;
        BinaryEncoder encoder = null;

        private byte[] serialize(GenericRecord record) throws IOException {
            try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                encoder = EncoderFactory.get().directBinaryEncoder(output, encoder);
                writer.write(record, encoder);
                return output.toByteArray();
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
