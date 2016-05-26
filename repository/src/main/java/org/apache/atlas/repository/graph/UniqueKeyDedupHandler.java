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
package org.apache.atlas.repository.graph;


import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.IDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UniqueKeyDedupHandler implements DedupHandler<ClassType, IReferenceableInstance> {

    private static final GraphHelper graphHelper = GraphHelper.getInstance();

    private static final Logger LOG = LoggerFactory.getLogger(UniqueKeyDedupHandler.class);

    @Override
    public boolean exists(final ClassType classType, final IReferenceableInstance instance) throws AtlasException {
        return vertex(classType, instance) != null;
    }

    @Override
    public Vertex vertex(final ClassType classType, final IReferenceableInstance instance) throws AtlasException {
        Vertex result = null;
        LOG.debug("Checking if there is an instance with the same unique attributes for instance {}", instance.toShortString());
        for (AttributeInfo attributeInfo : classType.fieldMapping().fields.values()) {
            if (attributeInfo.isUnique) {
                String propertyKey = graphHelper.getQualifiedFieldName(classType, attributeInfo.name);
                try {
                    result = graphHelper.getVertexForProperty(propertyKey, instance.get(attributeInfo.name));
                    LOG.debug("Found vertex by unique attribute : " + propertyKey + "=" + instance.get(attributeInfo.name));
                } catch (EntityNotFoundException e) {
                    //Its ok if there is no entity with the same unique value
                }
            }
        }
        return result;
    }
}
