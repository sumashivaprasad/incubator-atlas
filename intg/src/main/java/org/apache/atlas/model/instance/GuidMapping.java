/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.instance;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;

import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * This stores a mapping of guid assignments that were made during the processing
 * of a create or update entity request.
 *.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class GuidMapping {

    @JsonIgnore
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    private Map<String,String> guidAssignments;


    public GuidMapping() {

    }

    public GuidMapping(Map<String,String> guidAssignments) {
        this.guidAssignments = guidAssignments;
    }


    public Map<String,String> getGuidAssignments() {
        return guidAssignments;
    }

    public void setGuidAssignments(Map<String,String> guidAssignments) {
        this.guidAssignments = guidAssignments;
    }
    /**
     * Converts the GuidMapping to json
     */
    @Override
    public String toString() {
        return gson.toJson(this);
    }

    @JsonIgnore
    public static GuidMapping fromString(String json) {
        return gson.fromJson(json, GuidMapping.class);
    }

}