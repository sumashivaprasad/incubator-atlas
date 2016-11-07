/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.adapters.v1;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.web.adapters.AtlasFormatAdapter;
import org.apache.atlas.web.adapters.AtlasFormatConverters;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Singleton
public class StructToAtlasStructConverter implements AtlasFormatAdapter {

    protected AtlasTypeRegistry typeRegistry;
    protected AtlasFormatConverters registry;

    public static final String TARGET_VERSION = AtlasFormatConverters.VERSION_V2;

    @Inject
    StructToAtlasStructConverter(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Inject
    public void init(AtlasFormatConverters registry) throws AtlasBaseException {
        this.registry = registry;
        registry.registerConverter(this, TARGET_VERSION);
    }

    @Override
    public Object convert(final String targetVersion, final AtlasType type, final Object source) throws AtlasBaseException {

        if (source != null) {
            //Json unmarshalling gives us a Map instead of AtlasObjectId or AtlasEntity

            if (isStructType(source)) {

                Struct entity = (Struct) source;
                AtlasStructDef structDef = typeRegistry.getStructDefByName(entity.getTypeName());

                //Resolve attributes
                StructToAtlasStructConverter converter = (StructToAtlasStructConverter) registry.getConverter(TARGET_VERSION, AtlasType.TypeCategory.STRUCT);
                return new AtlasStruct(type.getTypeName(), converter.convertAttributes(structDef.getAttributeDefs(), entity));
            }

        }
        return null;
    }

    private boolean isStructType(Object o) {
        if (o != null && o instanceof Struct) {
            return true;
        }
        return false;
    }

    @Override
    public AtlasType.TypeCategory getTypeCategory() {
        return AtlasType.TypeCategory.STRUCT;
    }

    public Map<String, Object> convertAttributes(Collection<AtlasStructDef.AtlasAttributeDef> attributeDefs, Object entity) throws AtlasBaseException {
        Map<String, Object> newAttrMap = new HashMap<>();
        for (AtlasStructDef.AtlasAttributeDef attrDef : attributeDefs) {
            String attrTypeName = attrDef.getTypeName();
            AtlasType attrType = typeRegistry.getType(attrTypeName);
            AtlasType.TypeCategory typeCategory = attrType.getTypeCategory();

            AtlasFormatAdapter attrConverter = registry.getConverter(TARGET_VERSION, typeCategory);

            Object attrVal = null;
            if ( AtlasFormatConverters.isMapType(entity)) {
                attrVal = ((Map)entity).get(attrDef.getName());
            } else {
                attrVal = ((Struct)entity).get(attrDef.getName());
            }
            final Object convertedVal = attrConverter.convert(TARGET_VERSION, attrType, attrVal);
            newAttrMap.put(attrDef.getName(), convertedVal);
        }

        return newAttrMap;
    }
}
