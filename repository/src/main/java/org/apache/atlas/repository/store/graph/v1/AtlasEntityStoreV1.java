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


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.RequestContext;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasEntityWithAssociations;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.EntityGraphDiscovery;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.repository.store.graph.EntityResolver;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import com.google.inject.Inject;


public class AtlasEntityStoreV1 implements AtlasEntityStore {

    protected AtlasTypeRegistry typeRegistry;

    private EntityGraphMapper graphMapper;

    private DeleteHandlerV1 deleteHandler;

    private GraphHelper graphHelper = GraphHelper.getInstance();

    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityStoreV1.class);

    @Inject
    public AtlasEntityStoreV1(EntityGraphMapper vertexMapper, DeleteHandlerV1 deleteHandler) {
        this.graphMapper = vertexMapper;
        this.deleteHandler = deleteHandler;
    }

    @Inject
    public void init(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        this.typeRegistry = typeRegistry;
    }

    @Override
    public AtlasEntity getById(final String guid) {
        return null;
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse deleteById(final String guid) throws AtlasBaseException {

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        EntityMutationResponse response = new EntityMutationResponse();

        // Retrieve vertices for requested guids.
        Map<String, AtlasVertex> vertices = graphHelper.getVerticesForGUIDs(new ArrayList<String>() {{ add(guid); }} );
        Collection<AtlasVertex> deletionCandidates = vertices.values();

        if (LOG.isDebugEnabled()) {
                if(! vertices.containsKey(guid)) {
                    // Entity does not exist - treat as non-error, since the caller
                    // wanted to delete the entity and it's already gone.
                    LOG.debug("Deletion request ignored for non-existent entity with guid " + guid);
                }
        }

        if (deletionCandidates.isEmpty()) {
            LOG.info("No deletion candidate entities were found for guids %s", guid);
        }

        deleteHandler.deleteEntities(deletionCandidates);
        RequestContextV1 req = RequestContextV1.get();
        for (AtlasObjectId id : req.getDeletedEntityIds()) {
            response.addEntity(EntityMutations.EntityOperation.DELETE, AtlasEntityStoreV1.constructHeader(id));
        }

        for (AtlasObjectId id : req.getUpdatedEntityIds()) {
            response.addEntity(EntityMutations.EntityOperation.UPDATE, AtlasEntityStoreV1.constructHeader(id));
        }

        return response;
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse createOrUpdate(final Map<String, AtlasEntity> entities) throws AtlasBaseException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEntityStoreV1.createOrUpdate({}, {})", entities);
        }

        //Validate
        List<AtlasEntity> normalizedEntities = validateAndNormalize(entities);

        //Discover entities, create vertices
        EntityMutationContext ctx = preCreateOrUpdate(normalizedEntities);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.createOrUpdate({}, {}): {}", entities);
        }

        return graphMapper.mapAttributes(ctx);
    }

    @Override
    @GraphTransaction
    public AtlasEntity.AtlasEntities getByIds(final List<String> guid) throws AtlasBaseException {
        return null;
    }

    @Override
    @GraphTransaction
    public AtlasEntityWithAssociations getWithAssociationsByIds(final List<String> guid) throws AtlasBaseException {
        return null;
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse deleteByIds(final List<String> guid) throws AtlasBaseException {
        return null;
    }

    @Override
    @GraphTransaction
    public AtlasEntity getByUniqueAttribute(final String typeName, final String attrName, final String attrValue) {
        return null;
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse updateByUniqueAttribute(final String typeName, final String attributeName, final String attributeValue, final AtlasEntity entity) throws AtlasBaseException {
        return null;
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse deleteByUniqueAttribute(final String typeName, final String attributeName, final String attributeValue) throws AtlasBaseException {
        return null;
    }

    @Override
    @GraphTransaction
    public void addClassifications(final String guid, final List<AtlasClassification> classification) throws AtlasBaseException {

    }

    @Override
    @GraphTransaction
    public void updateClassifications(final String guid, final List<AtlasClassification> classification) throws AtlasBaseException {

    }

    @Override
    @GraphTransaction
    public void deleteClassifications(final String guid, final List<String> classificationNames) throws AtlasBaseException {

    }

    private EntityMutationContext preCreateOrUpdate(final List<AtlasEntity> atlasEntities) throws AtlasBaseException {
        List<EntityResolver> entityResolvers = new ArrayList<>();

        entityResolvers.add(new IDBasedEntityResolver());
        entityResolvers.add(new UniqAttrBasedEntityResolver(typeRegistry));

        EntityGraphDiscovery        graphDiscoverer    = new AtlasEntityGraphDiscoveryV1(typeRegistry, entityResolvers);
        EntityGraphDiscoveryContext discoveredEntities = graphDiscoverer.discoverEntities(atlasEntities);
        EntityMutationContext       context            = new EntityMutationContext(discoveredEntities);

        for (AtlasEntity entity : discoveredEntities.getRootEntities()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasEntityStoreV1.preCreateOrUpdate({}): {}", entity);
            }

            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

            if (entityType == null) {
                throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), entity.getTypeName());
            }

            final AtlasVertex vertex;
            AtlasObjectId     objId = entity.getAtlasObjectId();

            if (discoveredEntities.isResolvedId(objId) ) {
                vertex = discoveredEntities.getResolvedEntityVertex(objId);

                context.addUpdated(entity, entityType, vertex);

                String guid = AtlasGraphUtilsV1.getIdFromVertex(vertex);

                RequestContextV1.get().recordEntityUpdate(new AtlasObjectId(entityType.getTypeName(), guid));
            } else {
                //Create vertices which do not exist in the repository
                vertex = graphMapper.createVertexTemplate(entity, entityType);

                context.addCreated(entity, entityType, vertex);

                discoveredEntities.addResolvedId(objId, vertex);
                discoveredEntities.removeUnResolvedId(objId);

                String guid = AtlasGraphUtilsV1.getIdFromVertex(vertex);

                RequestContextV1.get().recordEntityCreate(new AtlasObjectId(entityType.getTypeName(), guid));
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasEntityStoreV1.preCreateOrUpdate({}): {}", entity, vertex);
            }
        }

        return context;
    }

    private List<AtlasEntity> validateAndNormalize(final Map<String, AtlasEntity> entities) throws AtlasBaseException {
        List<AtlasEntity> normalizedEntities = new ArrayList<>();
        List<String>      messages           = new ArrayList<>();

        for (String entityId : entities.keySet()) {
            if ( !AtlasEntity.isAssigned(entityId) && !AtlasEntity.isUnAssigned(entityId)) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_CRUD_INVALID_PARAMS, ": Guid in map key is invalid " + entityId);
            }

            AtlasEntity entity = entities.get(entityId);

            if ( entity == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_CRUD_INVALID_PARAMS, ": Entity is null for guid " + entityId);
            }

            AtlasEntityType type = typeRegistry.getEntityTypeByName(entity.getTypeName());
            if (type == null) {
                throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), entity.getTypeName());
            }

            type.validateValue(entity, entity.getTypeName(), messages);

            if ( !messages.isEmpty()) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_CRUD_INVALID_PARAMS, messages);
            }

            AtlasEntity normalizedEntity = (AtlasEntity) type.getNormalizedValue(entity);

            normalizedEntities.add(normalizedEntity);
        }

        return normalizedEntities;
    }

    public void cleanUp() throws AtlasBaseException {
    }

    public static AtlasEntityHeader constructHeader(AtlasObjectId id) {
        AtlasEntityHeader entity = new AtlasEntityHeader(id.getTypeName());
        entity.setGuid(id.getGuid());

        return entity;
    }
}
