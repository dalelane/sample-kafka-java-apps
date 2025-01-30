/**
 * Copyright 2025 IBM Corp. All Rights Reserved.
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
package com.ibm.eventautomation.demos.streamprocessors;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import com.ibm.eventautomation.demos.utils.Utils;

/**
 * Processes text messages from an input topic, and produces
 *  an upper-case copy to an output topic.
 */
public class TextProcessor {

    /** Config to use for the connection to the Kafka cluster. */
    private static final Path STREAMS_CONFIG = Paths.get("./testdata/streams.properties");

    /** Simple stateless transformation implementation - that upper-cases the value text. */
    private static final KeyValueMapper<String, String, KeyValue<String, String>> UPPER_CASE_TRANSFORM = new KeyValueMapper<>() {
        @Override
        public KeyValue<String, String> apply(String key, String value) {
            return new KeyValue<String,String>(key, value.toUpperCase());
        }
    };



    public static void run() throws IOException {

        // prepare serializer/deserializer for processing the JSON messages
        Serde<String> keySerde = Serdes.String();
        Serde<String> valueSerde = Serdes.String();

        // read Kafka client configuration from the properties file
        Properties kafkaConfig = Utils.readProperties(STREAMS_CONFIG);
        String INPUT_TOPIC = kafkaConfig.getProperty("input.topic");
        String OUTPUT_TOPIC = kafkaConfig.getProperty("output.topic");

        // define the streams filter
        StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(INPUT_TOPIC, Consumed.with(keySerde, valueSerde))
            .map(UPPER_CASE_TRANSFORM, Named.as("upper_case_text"))
            .to(OUTPUT_TOPIC, Produced.with(keySerde, valueSerde));

        // start the streams processor running
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, kafkaConfig);
        streams.start();

        // stop on ctrl-C
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
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
