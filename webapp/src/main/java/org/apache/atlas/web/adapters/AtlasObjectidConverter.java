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
package org.apache.atlas.web.adapters;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class AtlasObjectIdConverter extends  AtlasAbstractFormatConverter {

    public AtlasObjectIdConverter(AtlasFormatConverters registry, AtlasTypeRegistry typeRegistry) {
        this(registry, typeRegistry, TypeCategory.OBJECT_ID_TYPE);
    }

    protected AtlasObjectIdConverter(AtlasFormatConverters registry, AtlasTypeRegistry typeRegistry, TypeCategory typeCategory) {
        super(registry, typeRegistry, typeCategory);
    }

    @Override
    public Object fromV1ToV2(Object v1Obj, AtlasType type, AtlasFormatConverter.ConverterContext converterContext) throws AtlasBaseException {
        Object ret = null;

        if (v1Obj != null) {
            if (v1Obj instanceof Id) {
                Id id = (Id) v1Obj;
                ret = new AtlasObjectId(id.getTypeName(), id._getId());
            } else if (v1Obj instanceof IReferenceableInstance) {
                IReferenceableInstance entity = (IReferenceableInstance) v1Obj;
                ret = new AtlasObjectId(entity.getTypeName(), entity.getId()._getId());
            }
        }
        return ret;
    }

    @Override
    public Object fromV2ToV1(Object v2Obj, AtlasType type, ConverterContext converterContext) throws AtlasBaseException {
        Id ret = null;

        if (v2Obj != null) {

            if (v2Obj instanceof Map) {
                Map    v2Map    = (Map) v2Obj;
                String idStr    = (String)v2Map.get(AtlasObjectId.KEY_GUID);
                String typeName = type.getTypeName();

                if (StringUtils.isEmpty(idStr)) {
                    throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND);
                }

                ret = new Id(idStr, 0, typeName);
            } else if (v2Obj instanceof AtlasObjectId) { // transient-id
                ret = new Id(((AtlasObjectId) v2Obj).getGuid(), 0, type.getTypeName());
            } else if (v2Obj instanceof AtlasEntity) {
                AtlasEntity entity = (AtlasEntity) v2Obj;
                ret = new Id(((AtlasObjectId) v2Obj).getGuid(), 0, type.getTypeName());
            } else {
                throw new AtlasBaseException(AtlasErrorCode.TYPE_CATEGORY_INVALID, type.getTypeCategory().name());
            }
        }
        return ret;
    }
}
