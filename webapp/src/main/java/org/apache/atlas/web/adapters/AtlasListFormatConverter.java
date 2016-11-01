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
package org.apache.atlas.web.adapters;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.type.AtlasTypeRegistry;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public class AtlasListFormatConverter extends AtlasArrayFormatConverter {

    @Inject
    AtlasListFormatConverter(final AtlasTypeRegistry typeRegistry) {
        super(typeRegistry);
    }

    @Override
    public Object convert(final Object source) throws AtlasBaseException {
        List newList = new ArrayList();
        List originalList = (List) source;

        for (Object elem : originalList) {
            Object convertedKey = registry.getConverter(elem.getClass());
            newList.add(convertedKey);
        }

        return newList;
    }

    @Override
    public Class getSourceType() {
        return List.class;
    }

    @Override
    public Class getTargetType() {
        return List.class;
    }

}
