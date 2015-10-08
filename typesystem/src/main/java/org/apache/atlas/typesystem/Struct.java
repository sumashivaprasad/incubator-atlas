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

package org.apache.atlas.typesystem;

import org.apache.atlas.AtlasException;
import org.apache.atlas.classification.InterfaceAudience;

import java.util.HashMap;
import java.util.Map;

public class Struct implements IStruct {

    public final String typeName;
    private final Map<String, Object> values;

    public Struct(String typeName) {
        this.typeName = typeName;
        values = new HashMap<>();
    }

    @InterfaceAudience.Private
    public Struct(String typeName, Map<String, Object> values) {
        this(typeName);
        if (values != null) {
            this.values.putAll(values);
        }
    }

    @Override
    public String getTypeName() {
        return typeName;
    }

    @Override
    public Object get(String attrName) {
        return values.get(attrName);
    }

    @Override
    public void set(String attrName, Object value) {
        values.put(attrName, value);
    }

    @Override
    public void clear(String attrName) throws AtlasException {
        values.remove(attrName);
    }

    @Override
    public Map<String, Object> getValuesMap() {
        return values;
    }

    public boolean equalsContents(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null) {
            return false;
        }

        if (o.getClass() != getClass()) {
            return false;
        }

        Struct obj = (Struct)o;
        if(!typeName.equals(obj.getTypeName())) {
            return false;
        }

        if(!values.equals(obj.getValuesMap())) {
            return false;
        }

        return true;
    }

//    @Override
//    public boolean equals(Object o) {
//        if (this == o) {
//            return true;
//        }
//
//        if (o == null) {
//            return false;
//        }
//
//        if (o.getClass() != getClass()) {
//            return false;
//        }
//
//        Struct obj = (Struct)o;
//        if(!typeName.equals(obj.getTypeName())) {
//            return false;
//        }
//
//        if(!values.equals(obj.getValuesMap())) {
//            return false;
//        }
//
//        return true;
//    }
//
//    @Override
//    public int hashCode() {
//        int result = typeName.hashCode();
//        result = 31 * result + values.hashCode();
//        return result;
//    }
}
