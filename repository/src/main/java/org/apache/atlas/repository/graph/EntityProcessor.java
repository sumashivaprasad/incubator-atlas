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

import com.google.inject.Inject;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.ObjectGraphWalker;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public final class EntityProcessor implements ObjectGraphWalker.NodeProcessor {

    private  final Logger LOG = LoggerFactory.getLogger(TypedInstanceToGraphMapper.class);

    private final Map<Id, IReferenceableInstance> idToInstanceMap;
    private final TypedInstanceToGraphMapper.Operation operation;

    private static final GraphHelper graphHelper = GraphHelper.getInstance();

    public EntityProcessor(TypedInstanceToGraphMapper.Operation op) {
        idToInstanceMap = new LinkedHashMap<>();
        this.operation = op;
    }

    public void cleanUp() {
        idToInstanceMap.clear();
    }

    public  Map<Id, IReferenceableInstance> getIdToInstanceMap() {
        return idToInstanceMap;
    }

    public Collection<IReferenceableInstance> getInstances() {
        return idToInstanceMap.values();
    }

    @Override
    public void processNode(ObjectGraphWalker.Node nd) throws AtlasException {
        IReferenceableInstance ref = null;
        Id id = null;

        if (nd.attributeName == null) {
            ref = (IReferenceableInstance) nd.instance;
            id = ref.getId();

            LOG.debug("Processing " + nd + " for " + id);
        } else if (nd.aInfo.dataType().getTypeCategory() == DataTypes.TypeCategory.CLASS) {
            /* In case of an update Entity , do not attempt to update the related class attributes unless its a composite
             */
            if (nd.value != null && (nd.value instanceof Id)) {
                id = (Id) nd.value;
            }

            if (TypedInstanceToGraphMapper.Operation.UPDATE.equals(operation)) {
                if (id != null && id.isAssigned() && !nd.aInfo.isComposite) {
                    // has a GUID and it is not a composite relation. Then do not add instance for updation
                    return;
                } else if(ref != null){
                    //Check if there is already an instance with the same unique attribute value
                    ClassType classType = TypeSystem.getInstance().getDataType(ClassType.class, ref.getTypeName());
                    Vertex classVertex = graphHelper.getVertexForInstanceByUniqueAttribute(classType, ref);
                    //Vertex already exists and it is not a composite. Do not add for updation
                    if(classVertex != null && !nd.aInfo.isComposite) {
                        LOG.debug("Not processing " + nd + " since it is not a composite attribute for " + id);
                        return;
                    }
                }
            }
        }

        if (id != null) {
            if (id.isUnassigned()) {
                if (ref != null) {
                    if (idToInstanceMap.containsKey(id)) { // Oops
                        throw new RepositoryException(
                            String.format("Unexpected internal error: Id %s processed again", id));
                    }
                    LOG.debug("Added " + id + " for processing ");
                    idToInstanceMap.put(id, ref);
                }
            } else {
                //Updating entity
                if (ref != null) {
                    LOG.debug("Added " + id + " for processing ");
                    idToInstanceMap.put(id, ref);
                }
            }
        }
    }
}
