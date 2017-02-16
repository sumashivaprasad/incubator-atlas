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


import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.EntityGraphDiscovery;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;


@Singleton
public class AtlasEntityStoreV1 implements AtlasEntityStore {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityStoreV1.class);

    private final DeleteHandlerV1   deleteHandler;
    private final AtlasTypeRegistry typeRegistry;

    @Inject
    public AtlasEntityStoreV1(DeleteHandlerV1 deleteHandler, AtlasTypeRegistry typeRegistry) {
        this.deleteHandler = deleteHandler;
        this.typeRegistry  = typeRegistry;
    }

    @Override
    @GraphTransaction
    public AtlasEntityWithExtInfo getById(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getById({})", guid);
        }

        EntityGraphRetriever entityRetriever = new EntityGraphRetriever(typeRegistry);

        AtlasEntityWithExtInfo ret = entityRetriever.toAtlasEntityWithExtInfo(guid);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getById({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntitiesWithExtInfo getByIds(List<String> guids) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getByIds({})", guids);
        }

        EntityGraphRetriever entityRetriever = new EntityGraphRetriever(typeRegistry);

        AtlasEntitiesWithExtInfo ret = entityRetriever.toAtlasEntitiesWithExtInfo(guids);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getByIds({}): {}", guids, ret);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntityWithExtInfo getByUniqueAttributes(AtlasEntityType entityType, Map<String, Object> uniqAttributes)
                                                                                            throws AtlasBaseException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getByUniqueAttribute({}, {})", entityType.getTypeName(), uniqAttributes);
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV1.getVertexByUniqueAttributes(entityType, uniqAttributes);

        EntityGraphRetriever entityRetriever = new EntityGraphRetriever(typeRegistry);

        AtlasEntityWithExtInfo ret = entityRetriever.toAtlasEntityWithExtInfo(entityVertex);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, entityType.getTypeName(),
                uniqAttributes.toString());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getByUniqueAttribute({}, {}): {}", entityType.getTypeName(), uniqAttributes, ret);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse createOrUpdate(EntityStream entityStream, boolean isPartialUpdate) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> createOrUpdate()");
        }

        if (entityStream == null || !entityStream.hasNext()) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "no entities to create/update.");
        }

        // Create/Update entities
        EntityGraphMapper entityGraphMapper = new EntityGraphMapper(deleteHandler, typeRegistry);

        EntityMutationContext context = preCreateOrUpdate(entityStream, entityGraphMapper, isPartialUpdate);

        EntityMutationResponse ret = entityGraphMapper.mapAttributes(context, isPartialUpdate);

        ret.setGuidAssignments(context.getGuidAssignments());

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== createOrUpdate()");
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse updateByUniqueAttributes(AtlasEntityType entityType, Map<String, Object> uniqAttributes,
                                                           AtlasEntity updatedEntity) throws AtlasBaseException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> updateByUniqueAttributes({}, {})", entityType.getTypeName(), uniqAttributes);
        }

        if (updatedEntity == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "no entity to update.");
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV1.getVertexByUniqueAttributes(entityType, uniqAttributes);

        updatedEntity.setGuid(AtlasGraphUtilsV1.getIdFromVertex(entityVertex));

        return createOrUpdate(new AtlasEntityStream(updatedEntity), true);
    }

    @GraphTransaction
    public EntityMutationResponse deleteById(final String guid) throws AtlasBaseException {

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        // Retrieve vertices for requested guids.
        AtlasVertex vertex = AtlasGraphUtilsV1.findByGuid(guid);

        if (LOG.isDebugEnabled()) {
            if (vertex == null) {
                // Entity does not exist - treat as non-error, since the caller
                // wanted to delete the entity and it's already gone.
                LOG.debug("Deletion request ignored for non-existent entity with guid " + guid);
            }
        }

        Collection<AtlasVertex> deletionCandidates = new ArrayList<AtlasVertex>();
        deletionCandidates.add(vertex);

        return deleteVertices(deletionCandidates);
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse deleteByIds(final List<String> guids) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(guids)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Guid(s) not specified");
        }

        Collection<AtlasVertex> deletionCandidates = new ArrayList<>();

        for (String guid : guids) {
            // Retrieve vertices for requested guids.
            AtlasVertex vertex = AtlasGraphUtilsV1.findByGuid(guid);
            if (LOG.isDebugEnabled()) {
                if (vertex == null) {
                    // Entity does not exist - treat as non-error, since the caller
                    // wanted to delete the entity and it's already gone.
                    LOG.debug("Deletion request ignored for non-existent entity with guid " + guid);
                }
            }
            deletionCandidates.add(vertex);

        }

        if (deletionCandidates.isEmpty()) {
            LOG.info("No deletion candidate entities were found for guids %s", guids);
        }
        return deleteVertices(deletionCandidates);
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse deleteByUniqueAttributes(AtlasEntityType entityType, Map<String, Object> uniqAttributes)
            throws AtlasBaseException {

        if (MapUtils.isEmpty(uniqAttributes)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, uniqAttributes.toString());
        }

        final AtlasVertex vertex = AtlasGraphUtilsV1.findByUniqueAttributes(entityType, uniqAttributes);
        Collection<AtlasVertex> deletionCandidates = new ArrayList<>();
        deletionCandidates.add(vertex);

        return deleteVertices(deletionCandidates);
    }

    @Override
    @GraphTransaction
    public void addClassifications(String guid, List<AtlasClassification> classification) throws AtlasBaseException {
        throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "addClassifications() not implemented yet");
    }

    @Override
    @GraphTransaction
    public void updateClassifications(String guid, List<AtlasClassification> classification) throws AtlasBaseException {
        throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "updateClassifications() not implemented yet");
    }

    @Override
    @GraphTransaction
    public void deleteClassifications(String guid, List<String> classificationNames) throws AtlasBaseException {
        throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "deleteClassifications() not implemented yet");
    }


    private EntityMutationContext preCreateOrUpdate(EntityStream entityStream, EntityGraphMapper entityGraphMapper, boolean isPartialUpdate) throws AtlasBaseException {
        EntityGraphDiscovery        graphDiscoverer  = new AtlasEntityGraphDiscoveryV1(typeRegistry, entityStream);
        EntityGraphDiscoveryContext discoveryContext = graphDiscoverer.discoverEntities();
        EntityMutationContext       context          = new EntityMutationContext(discoveryContext);

        for (String guid : discoveryContext.getReferencedGuids()) {
            AtlasVertex vertex = discoveryContext.getResolvedEntityVertex(guid);
            AtlasEntity entity = entityStream.getByGuid(guid);

            if (entity != null) {
                
                if (vertex != null) {
                    // entity would be null if guid is not in the stream but referenced by an entity in the stream
                    if (!isPartialUpdate) {
                        graphDiscoverer.validateAndNormalize(entity);
                    } else {
                        graphDiscoverer.validateAndNormalizeForUpdate(entity);
                    }

                    AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

                    String guidVertex = AtlasGraphUtilsV1.getIdFromVertex(vertex);

                    if (!StringUtils.equals(guidVertex, guid)) { // if entity was found by unique attribute
                        entity.setGuid(guidVertex);
                    }

                    context.addUpdated(guid, entity, entityType, vertex);
                } else {
                    graphDiscoverer.validateAndNormalize(entity);

                    AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

                    //Create vertices which do not exist in the repository
                    if ((entityStream instanceof EntityImportStream) && AtlasEntity.isAssigned(entity.getGuid())) {
                        vertex = entityGraphMapper.createVertexWithGuid(entity, entity.getGuid());
                    } else {
                        vertex = entityGraphMapper.createVertex(entity);
                    }

                    discoveryContext.addResolvedGuid(guid, vertex);

                    String generatedGuid = AtlasGraphUtilsV1.getIdFromVertex(vertex);

                    entity.setGuid(generatedGuid);

                    context.addCreated(guid, entity, entityType, vertex);
                }
            }
        }

        return context;
    }

    private EntityMutationResponse deleteVertices(Collection<AtlasVertex> deletionCandidates) throws AtlasBaseException {
        EntityMutationResponse response = new EntityMutationResponse();
        deleteHandler.deleteEntities(deletionCandidates);
        RequestContextV1 req = RequestContextV1.get();
        for (AtlasObjectId id : req.getDeletedEntityIds()) {
            response.addEntity(EntityMutations.EntityOperation.DELETE, EntityGraphMapper.constructHeader(id));
        }

        for (AtlasObjectId id : req.getUpdatedEntityIds()) {
            response.addEntity(EntityMutations.EntityOperation.UPDATE, EntityGraphMapper.constructHeader(id));
        }

        return response;
    }
}
