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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;

import com.ibm.eventautomation.demos.streamprocessors.data.JsonDataItem;
import com.ibm.eventautomation.demos.streamprocessors.serdes.GsonSerdes;
import com.ibm.eventautomation.demos.utils.Utils;

/**
 * Processes JSON messages from an input topic, and produces
 *  a filtered subset of them to an output topic.
 *
 * The source code needs to be modified to align with the JSON
 *  messages to be processed.
 */
public class JsonProcessor {

    /** Config to use for the connection to the Kafka cluster. */
    private static final Path STREAMS_CONFIG = Paths.get("./testdata/streams.properties");

    /**
     * Simple stateless filter implementation - based on the
     *  sample JSON payloads described in JsonDataItem.
     *
     * TODO To process different JSON data payloads
     *  the JsonDataItem class definition will need to be
     *  updated, and this filter will need to be updated to
     *  match a property that is available in the data
     */
    private static final Predicate<String, JsonDataItem> HIGH_RATINGS_ONLY = new Predicate<String, JsonDataItem>() {
        @Override
        public boolean test(String key, JsonDataItem value) {
            return value.getRating() > 4;
        }
    };



    public static void run() throws IOException {

        // prepare serializer/deserializer for processing the JSON messages
        Serde<String> keySerde = Serdes.String();
        Serde<JsonDataItem> valueSerde = GsonSerdes.createSerdes(JsonDataItem.class);

        // read Kafka client configuration from the properties file
        Properties kafkaConfig = Utils.readProperties(STREAMS_CONFIG);
        String INPUT_TOPIC = kafkaConfig.getProperty("input.topic");
        String OUTPUT_TOPIC = kafkaConfig.getProperty("output.topic");

        // define the streams filter
        StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(INPUT_TOPIC, Consumed.with(keySerde, valueSerde))
            .filter(HIGH_RATINGS_ONLY, Named.as("high_ratings_only"))
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
