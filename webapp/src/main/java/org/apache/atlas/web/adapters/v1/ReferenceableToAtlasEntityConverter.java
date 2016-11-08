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
package org.apache.atlas.web.adapters.v1;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.web.adapters.AtlasFormatAdapter;
import org.apache.atlas.web.adapters.AtlasFormatConverters;

import javax.inject.Inject;
import java.util.Collection;
import java.util.Map;

public class ReferenceableToAtlasEntityConverter implements AtlasFormatAdapter {

    protected AtlasTypeRegistry typeRegistry;
    protected AtlasFormatConverters registry;

    @Inject
    ReferenceableToAtlasEntityConverter(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Inject
    public void init(AtlasFormatConverters registry) throws AtlasBaseException {
        this.registry = registry;
        registry.registerConverter(this, AtlasFormatConverters.VERSION_V1, AtlasFormatConverters.VERSION_V2);
    }

    @Override
    public Object convert(final String sourceVersion, final String targetVersion, final AtlasType type, final Object source) throws AtlasBaseException {

        if ( source != null) {

            if ( isId(source)) {

                Id idObj = (Id) source;
                AtlasEntity result = new AtlasEntity(idObj.getTypeName());
                setId(idObj, result);
                return result;
            } else if ( isEntityType(source) ) {

                IReferenceableInstance entity = (IReferenceableInstance) source;
                AtlasEntityType entityType = (AtlasEntityType) typeRegistry.getType(entity.getTypeName());

                //Resolve attributes
                StructToAtlasStructConverter converter = (StructToAtlasStructConverter) registry.getConverter(sourceVersion, targetVersion, AtlasType.TypeCategory.STRUCT);
                AtlasEntity result =  new AtlasEntity(entity.getTypeName(), converter.convertAttributes(entityType.getAllAttributeDefs().values(), entity));
                setId(entity, result);
                return  result;
            }
        }
        return null;
    }

    private boolean isEntityType(Object o) {
        if ( o != null && o instanceof IReferenceableInstance) {
            return true;
        }
        return false;
    }

    private boolean isId(Object o) {
        if ( o != null && o instanceof Id) {
            return true;
        }
        return false;
    }

    @Override
    public AtlasType.TypeCategory getTypeCategory() {
        return AtlasType.TypeCategory.ENTITY;
    }


    private void setId(IReferenceableInstance entity, AtlasEntity result) {
        result.setGuid(entity.getId()._getId());
    }
}
