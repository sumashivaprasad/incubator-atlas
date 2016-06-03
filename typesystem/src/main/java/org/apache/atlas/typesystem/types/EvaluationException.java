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

import org.apache.atlas.AtlasException;

public class EvaluationException extends AtlasException {

    public EvaluationException(IDataType typ, Object val) {
        this(typ, val, (Throwable) null);
    }

    public EvaluationException(IDataType typ, Object val, Throwable t) {
        super(String.format("Cannot convert value '%s' to datatype %s", val.toString(), typ.getName()), t);
    }

    public EvaluationException(IDataType typ, Object val, String msg) {
        super(String
                .format("Cannot convert value '%s' to datatype %s because: %s", val.toString(), typ.getName(), msg));
    }

    public EvaluationException(String typeName, Object val, String msg) {
        super(String.format("Cannot convert value '%s' to datatype %s because: %s", val.toString(), typeName, msg));
    }

    protected EvaluationException(String msg) {
        super(msg);
    }

    protected EvaluationException(String msg, Exception e) {
        super(msg, e);
    }

}
