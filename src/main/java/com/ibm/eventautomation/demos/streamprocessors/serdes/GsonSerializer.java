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
package com.ibm.eventautomation.demos.streamprocessors.serdes;

import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonSerializer<T> implements Serializer<T> {

    private Class<T> target;
    private Gson jsonParser;

    public GsonSerializer(Class<T> targetClass) {
        GsonBuilder gsonBuilder = new GsonBuilder();

        jsonParser = gsonBuilder.create();
        target = targetClass;
    }

    @Override
    public byte[] serialize(String topic, T data) {
        return jsonParser.toJson(data, target).getBytes();
    }
}
