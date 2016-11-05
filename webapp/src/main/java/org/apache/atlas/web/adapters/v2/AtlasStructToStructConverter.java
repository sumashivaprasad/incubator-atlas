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
package org.apache.atlas.web.adapters.v2;

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
public class AtlasStructToStructConverter implements AtlasFormatAdapter {

    protected AtlasTypeRegistry typeRegistry;
    protected AtlasFormatConverters registry;

    public static final String ATTRIBUTES_PROPERTY_KEY = "attributes";
    public static final String TRANSIENT_ID="transientId";

    @Inject
    AtlasStructToStructConverter(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Inject
    public void init(AtlasFormatConverters registry) throws AtlasBaseException {
        this.registry = registry;
        registry.registerConverter(this, AtlasFormatConverters.VERSION_V1);
    }

    @Override
    public Object convert(final String targetVersion, final AtlasType type, final Object source) throws AtlasBaseException {

        if (source != null) {
            //Json unmarshalling gives us a Map instead of AtlasObjectId or AtlasEntity
            if (AtlasFormatConverters.isMapType(source)) {
                //Could be an entity or an Id
                Map srcMap = (Map) source;
                AtlasStructDef structDef = ((AtlasStructType) type).getStructDefinition();

                final Map attrMap = (Map) srcMap.get(ATTRIBUTES_PROPERTY_KEY);

                if ( attrMap != null) {
                    //Resolve attributes
                    AtlasStructToStructConverter converter = (AtlasStructToStructConverter) registry.getConverter(AtlasFormatConverters.VERSION_V1, AtlasType.TypeCategory.STRUCT);
                    return new Struct(type.getTypeName(), converter.convertAttributes(structDef.getAttributeDefs(), attrMap));
                }

            }
        } else if (isStructType(source)) {

            AtlasStruct entity = (AtlasStruct) source;
            AtlasStructDef structDef = typeRegistry.getStructDefByName(entity.getTypeName());

            //Resolve attributes
            AtlasStructToStructConverter converter = (AtlasStructToStructConverter) registry.getConverter(AtlasFormatConverters.VERSION_V1, AtlasType.TypeCategory.STRUCT);
            return new Struct(type.getTypeName(), converter.convertAttributes(structDef.getAttributeDefs(), entity));
        }

        return null;

    }

    private boolean isStructType(Object o) {
        if (o != null && o instanceof AtlasStruct) {
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

            AtlasFormatAdapter attrConverter = registry.getConverter(AtlasFormatConverters.VERSION_V1, typeCategory);

            Object attrVal = null;
            if ( AtlasFormatConverters.isMapType(entity)) {
                attrVal = ((Map)entity).get(attrDef.getName());
            } else {
                attrVal = ((AtlasStruct)entity).getAttribute(attrDef.getName());
            }
            final Object convertedVal = attrConverter.convert(AtlasFormatConverters.VERSION_V1, attrType, attrVal);
            newAttrMap.put(attrDef.getName(), convertedVal);
        }

        return newAttrMap;
    }
}
