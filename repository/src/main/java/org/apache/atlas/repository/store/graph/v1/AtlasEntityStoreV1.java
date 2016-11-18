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
package org.apache.atlas.repository.store.graph.v1;


import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityWithAssociations;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.DiscoveredEntities;
import org.apache.atlas.repository.store.graph.EntityGraphDiscovery;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class AtlasEntityStoreV1 implements AtlasEntityStore {

    protected EntityGraphDiscovery graphDiscoverer;
    protected AtlasTypeRegistry typeRegistry;

    @Inject
    EntityVertexMapper vertexMapper;

    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityStoreV1.class);

    @Override
    @Inject
    public void init(AtlasTypeRegistry typeRegistry, EntityGraphDiscovery graphDiscoverer) throws AtlasBaseException {
        this.graphDiscoverer = graphDiscoverer;
        this.typeRegistry = typeRegistry;
    }

    @Override
    public EntityMutationResponse createOrUpdate(final AtlasEntity entity) {
        return null;
    }

    private static class EntityMutationContext {
        private EntityMutations.EntityOperation op;
        private Map<EntityMutations.EntityOperation, List<AtlasEntity>> entitiesCreated  = new HashMap<>();
        private Map<EntityMutations.EntityOperation, List<AtlasEntity>> entitiesUpdated  = new HashMap<>();

        private Map<AtlasEntity, AtlasType> entityVsType = new HashMap<>();

        private Map<AtlasEntity, AtlasVertex> entityVsVertex = new HashMap<>();

        public void addCreated(AtlasEntity entity, AtlasType type, AtlasVertex atlasVertex) {
            if (!entitiesCreated.containsKey(EntityMutations.EntityOperation.CREATE)) {
                entitiesCreated.put(EntityMutations.EntityOperation.CREATE, new ArrayList<AtlasEntity>());
            }

            entitiesUpdated.get(EntityMutations.EntityOperation.CREATE).add(entity);
            entityVsVertex.put(entity, atlasVertex);
            entityVsType.put(entity, type);
        }

        public void addUpdated(AtlasEntity entity, AtlasType type, AtlasVertex atlasVertex) {
            if (!entitiesUpdated.containsKey(EntityMutations.EntityOperation.UPDATE)) {
                entitiesUpdated.put(EntityMutations.EntityOperation.UPDATE, new ArrayList<AtlasEntity>());
            }

            entitiesUpdated.get(EntityMutations.EntityOperation.UPDATE).add(entity);
            entityVsVertex.put(entity, atlasVertex);
            entityVsType.put(entity, type);
        }

        public Collection<AtlasEntity> getCreatedEntities() {
            return entitiesCreated.get(EntityMutations.EntityOperation.CREATE);
        }

        public Collection<AtlasEntity> getUpdatedEntities() {
            return entitiesUpdated.get(EntityMutations.EntityOperation.UPDATE);
        }

        AtlasType getType(AtlasEntity entity) {
            return entityVsType.get(entity);
        }

        AtlasVertex getVertex(AtlasEntity entity) {
            return entityVsVertex.get(entity);
        }
    }

    public EntityMutationContext preCreateOrUpdate(final List<AtlasEntity> atlasEntities) throws AtlasBaseException {

        EntityMutationContext context = new EntityMutationContext();

        DiscoveredEntities discoveredEntities = graphDiscoverer.discoverEntities(atlasEntities);

        for (AtlasEntity entity : discoveredEntities.getRootEntities()) {

            AtlasVertex vertex = null;
            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasEntityStoreV1.preCreate({}): {}", entity);
            }

            AtlasEntityType entityType = (AtlasEntityType) typeRegistry.getType(entity.getTypeName());

            if ( discoveredEntities.isResolved(entity) ) {
                //Create vertices which do not exist in the repository
                vertex = discoveredEntities.getResolvedReference(entity);
                context.addUpdated(entity, entityType, vertex);
            } else {
                vertex = vertexMapper.createVertex(entity, entityType);
                context.addCreated(entity, entityType, vertex);
            }

            //Map only primitive attributes
            vertexMapper.mapByCategory(vertex, entity, entityType, new HashSet<TypeCategory>() {{ add(TypeCategory.PRIMITIVE); add(TypeCategory.ENUM); }});

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasEntityStoreV1.preCreate({}): {}", entity, vertex);
            }
        }
        return context;
    }

    @Override
    public EntityMutationResponse updateById(final String guid, final AtlasEntity entity) {
        return null;
    }

    @Override
    public AtlasEntity getById(final String guid) {
        return null;
    }

    @Override
    public EntityMutationResponse deleteById(final String guid) {
        return null;
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse createOrUpdate(final List<AtlasEntity> entities) throws AtlasBaseException {

        EntityMutationResponse  resp = new EntityMutationResponse();
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEntityStoreV1.createOrUpdate({}, {})", entities);
        }

        List<AtlasEntity> normalizedEntities = normalize(entities);

        EntityMutationContext ctx = preCreateOrUpdate(normalizedEntities);

        final Collection<AtlasEntity> createdEntities = ctx.getCreatedEntities();
        for (AtlasEntity createdEntity : createdEntities) {
            vertexMapper.mapAttributestoVertex(createdEntity, ctx.getVertex(createdEntity), (AtlasStructType) ctx.getType(createdEntity));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.createOrUpdate({}, {}): {}", entities, vertex);
        }

        return resp;
    }

    @Override
    public EntityMutationResponse updateByIds(final String guid, final AtlasEntity entity) throws AtlasBaseException {
        return null;
    }

    @Override
    public AtlasEntity.AtlasEntities getByIds(final List<String> guid) throws AtlasBaseException {
        return null;
    }

    @Override
    public AtlasEntityWithAssociations getWithAssociationsByIds(final List<String> guid) throws AtlasBaseException {
        return null;
    }

    @Override
    public EntityMutationResponse deleteByIds(final List<String> guid) throws AtlasBaseException {
        return null;
    }

    @Override
    public AtlasEntity getByUniqueAttribute(final String typeName, final String attrName, final String attrValue) {
        return null;
    }

    @Override
    public EntityMutationResponse updateByUniqueAttribute(final String typeName, final String attributeName, final String attributeValue, final AtlasEntity entity) throws AtlasBaseException {
        return null;
    }

    @Override
    public EntityMutationResponse deleteByUniqueAttribute(final String typeName, final String attributeName, final String attributeValue) throws AtlasBaseException {
        return null;
    }

    @Override
    public EntityMutationResponse batchMutate(final EntityMutations mutations) throws AtlasBaseException {
        return null;
    }


    @Override
    public void addClassifications(final String guid, final List<AtlasClassification> classification) throws AtlasBaseException {

    }

    @Override
    public void updateClassifications(final String guid, final List<AtlasClassification> classification) throws AtlasBaseException {

    }

    @Override
    public void deleteClassifications(final String guid, final List<String> classificationNames) throws AtlasBaseException {

    }

    @Override
    public AtlasEntity.AtlasEntities searchEntities(final SearchFilter searchFilter) throws AtlasBaseException {
        return null;
    }

    private List<AtlasEntity> normalize(final List<AtlasEntity> entities) throws AtlasBaseException {

        List<AtlasEntity> normalizedEntities = new ArrayList<>();

        for (AtlasEntity entity : entities) {
            AtlasType type = typeRegistry.getType(entity.getTypeName());
            if (type.getTypeCategory() != TypeCategory.ENTITY) {
                throw new AtlasBaseException(AtlasErrorCode.TYPE_MATCH_FAILED, type.getTypeCategory().name(), TypeCategory.ENTITY.name());
            }

            AtlasEntity normalizedEntity = (AtlasEntity) type.getNormalizedValue(entity);
            normalizedEntities.add(normalizedEntity);
        }

        return normalizedEntities;
    }

}
