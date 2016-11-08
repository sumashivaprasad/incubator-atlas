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
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static org.apache.atlas.web.adapters.AtlasFormatConverters.VERSION_V1;
import static org.apache.atlas.web.adapters.AtlasFormatConverters.VERSION_V2;

public class AtlasArrayFormatConverter implements AtlasFormatAdapter {

    protected AtlasTypeRegistry typeRegistry;
    protected AtlasFormatConverters registry;

    @Inject
    AtlasArrayFormatConverter(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Inject
    public void init(AtlasFormatConverters registry) throws AtlasBaseException {
        this.registry = registry;
        registry.registerConverter(this, AtlasFormatConverters.VERSION_V1, AtlasFormatConverters.VERSION_V2);
        registry.registerConverter(this, AtlasFormatConverters.VERSION_V2, AtlasFormatConverters.VERSION_V1);
    }

    @Override
    public AtlasType.TypeCategory getTypeCategory() {
        return AtlasType.TypeCategory.ARRAY;
    }

    @Override
    public Object convert(String sourceVersion, String targetVersion, AtlasType type, final Object source) throws AtlasBaseException {

        Object normalizedValue = type.getNormalizedValue(source);
        Collection newCollection = null;
        if ( normalizedValue != null ) {
            if (isArrayListType(normalizedValue.getClass())) {
                newCollection = new ArrayList();
            } else if (isSetType(normalizedValue.getClass())) {
                newCollection = new LinkedHashSet();
            }

            AtlasArrayType arrType = (AtlasArrayType) type;
            AtlasType elemType = arrType.getElementType();

            Collection originalList = (Collection) normalizedValue;
            for (Object elem : originalList) {
                AtlasFormatAdapter elemConverter = registry.getConverter(sourceVersion, targetVersion, elemType.getTypeCategory());
                Object convertedVal = elemConverter.convert(sourceVersion, targetVersion, elemType, elem);

                newCollection.add(convertedVal);
            }
        }
        return newCollection;
    }

    public static boolean isArrayListType(Class c) {
        return List.class.isAssignableFrom(c);
    }

    public static boolean isSetType(Class c) {
        return Set.class.isAssignableFrom(c);
    }
}

