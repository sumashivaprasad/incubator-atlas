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
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.persistence.Id;

import javax.inject.Inject;

public class AtlasEntityToReferenceableConverter implements AtlasFormatAdapter<AtlasEntity, Referenceable> {

    protected AtlasTypeRegistry typeRegistry;
    protected AtlasFormatConverters registry;

    @Inject
    AtlasEntityToReferenceableConverter(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Inject
    public void init(AtlasFormatConverters registry) throws AtlasBaseException {
        this.registry = registry;
        registry.registerConverter(this);
    }

    @Override
    public Referenceable convert(final AtlasEntity source) throws AtlasBaseException {
        Referenceable ref = new Referenceable(source.getGuid(), source.getTypeName());

        AtlasFormatAdapter structConverter = registry.getConverter(AtlasStruct.class);

        final Struct struct = (Struct) structConverter.convert(source);

        for (String attrName : source.getAttributes().keySet()) {
            ref.set(attrName, struct.get(attrName));
        }
        return ref;
    }

    @Override
    public Class getSourceType() {
        return AtlasEntity.class;
    }

    @Override
    public Class getTargetType() {
        return Referenceable.class;
    }

    @Override
    public AtlasType.TypeCategory getTypeCategory() {
        return AtlasType.TypeCategory.ENTITY;
    }
}
