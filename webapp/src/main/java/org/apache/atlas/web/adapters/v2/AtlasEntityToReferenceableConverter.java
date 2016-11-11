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
package org.apache.atlas.web.adapters.v2;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.web.adapters.AtlasFormatAdapter;
import org.apache.atlas.web.adapters.AtlasFormatConverters;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import java.util.Collection;
import java.util.Map;

public class AtlasEntityToReferenceableConverter implements AtlasFormatAdapter {

    protected AtlasTypeRegistry typeRegistry;
    protected AtlasFormatConverters registry;


    @Inject
    AtlasEntityToReferenceableConverter(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Inject
    public void init(AtlasFormatConverters registry) throws AtlasBaseException {
        this.registry = registry;
        registry.registerConverter(this, AtlasFormatConverters.VERSION_V2, AtlasFormatConverters.VERSION_V1);
    }

    @Override
    public Object convert(final String sourceVersion, final String targetVersion, final AtlasType type, final Object source) throws AtlasBaseException {

        if ( source != null) {
            //JSOn unmarshalling gives us a Map instead of AtlasObjectId or AtlasEntity
            if ( AtlasFormatConverters.isMapType(source)) {
                //Could be an entity or an Id

                Map srcMap = (Map) source;
                String idStr = (String)srcMap.get(AtlasObjectId.KEY_GUID);
                String typeName = type.getTypeName();

                if (StringUtils.isEmpty(idStr)) {
                    throw new AtlasBaseException(AtlasErrorCode.ENTITY_GUID_NOT_FOUND);
                }

                if (MapUtils.isEmpty((Map)srcMap.get(AtlasStructToStructConverter.ATTRIBUTES_PROPERTY_KEY))) {
                    //Convert to Id
                    Id id = new Id(idStr, 0, typeName);
                    return id;
                } else {
                    AtlasEntityType entityType = (AtlasEntityType) typeRegistry.getType(type.getTypeName());
                    final Map attrMap = (Map) srcMap.get(AtlasStructToStructConverter.ATTRIBUTES_PROPERTY_KEY);
                    //Resolve attributes
                    AtlasStructToStructConverter converter = (AtlasStructToStructConverter) registry.getConverter(AtlasFormatConverters.VERSION_V2, AtlasFormatConverters.VERSION_V1, TypeCategory.STRUCT);
                    return new Referenceable(idStr, typeName, converter.convertAttributes(entityType, attrMap));

                }
            } else {

                AtlasEntityType entityType = (AtlasEntityType) typeRegistry.getType(type.getTypeName());
                if ( isEntityType(source) ) {
                    AtlasEntity entity = (AtlasEntity) source;
                    String id = entity.getGuid();
                    //Resolve attributes
                    AtlasStructToStructConverter converter = (AtlasStructToStructConverter) registry.getConverter(AtlasFormatConverters.VERSION_V2, AtlasFormatConverters.VERSION_V1, TypeCategory.STRUCT);
                    return new Referenceable(id, entity.getTypeName(), converter.convertAttributes(entityType, entity));

                } else if (isTransientId(source)) {
                    return new Referenceable((String) source, type.getTypeName(), null);
                }
            }
        }
        return null;
    }

    private boolean isEntityType(Object o) {
        if ( o != null && (o instanceof AtlasEntity)) {
            return true;
        }
        return false;
    }

    private boolean isTransientId(Object o) {
        if ( o != null && (o instanceof String)) {
            return true;
        }
        return false;
    }

    @Override
    public TypeCategory getTypeCategory() {
        return TypeCategory.ENTITY;
    }
}
