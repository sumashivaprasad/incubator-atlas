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

package org.apache.atlas.typesystem.types;

import org.apache.atlas.ParamChecker;

public class EnumValue {

    public final String value;
    public final int ordinal;

    public EnumValue(String value, int ordinal) {
        this.value = ParamChecker.notEmpty(value, "Enum value");
        this.ordinal = ordinal;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EnumValue enumValue = (EnumValue) o;

        if (ordinal != enumValue.ordinal) {
            return false;
        }
        if (!value.equals(enumValue.value)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = value.hashCode();
        result = 31 * result + ordinal;
        return result;
    }

    @Override
    public String toString() {
        return value;
    }
}
