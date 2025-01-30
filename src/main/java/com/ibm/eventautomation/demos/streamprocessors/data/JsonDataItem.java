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
package com.ibm.eventautomation.demos.streamprocessors.data;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;


/**
 * Java representation of JSON messages to be processed
 *
 * TODO To process different JSON data payloads
 *  this class will need to be updated to describe the
 *  properties in the messages
 */
public class JsonDataItem {

    @SerializedName("title")
    @Expose
    private String title;

    @SerializedName("text")
    @Expose
    private String text;

    @SerializedName("rating")
    @Expose
    private double rating;

    @SerializedName("loyalty")
    @Expose
    private String loyalty;


    public String getTitle() {
        return title;
    }
    public String getText() {
        return text;
    }
    public double getRating() {
        return rating;
    }
    public String getLoyalty() {
        return loyalty;
    }
}
