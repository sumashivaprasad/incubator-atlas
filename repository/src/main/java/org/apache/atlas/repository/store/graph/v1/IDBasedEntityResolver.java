package org.apache.atlas.repository.store.graph.v1;

import com.google.common.base.Optional;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.repository.store.graph.EntityResolver;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.persistence.Id;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IDBasedEntityResolver implements EntityResolver {

    private Map<String, AtlasEntity> idToEntityMap = new HashMap<>();

    private GraphHelper graphHelper = GraphHelper.getInstance();

    private EntityGraphDiscoveryContext context;

    @Override
    public void init(EntityGraphDiscoveryContext context) throws AtlasBaseException {
        this.context = context;
        for (AtlasEntity entity : context.getRootEntities()) {

            idToEntityMap.put(entity.getGuid(), entity);
        }
    }

    public EntityGraphDiscoveryContext resolveEntityReferences() throws AtlasBaseException {

        if ( context == null) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Entity resolver not initialized");
        }

        List<AtlasObjectId> resolvedReferences = new ArrayList<>();

        for (AtlasObjectId typeIdPair : context.getUnresolvedIdReferences()) {
            if ( AtlasEntity.isAssigned(typeIdPair.getGuid())) {
                //validate in graph repo that given guid, typename exists
                Optional<AtlasVertex> vertex = resolveGuid(typeIdPair);

                if ( vertex.isPresent() ) {
                    context.addRepositoryResolvedReference(typeIdPair, vertex.get());
                    resolvedReferences.add(typeIdPair);
                }
            } else {
                //check if root references have this temporary id
               if (!idToEntityMap.containsKey(typeIdPair.getGuid()) ) {
                   throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, "Could not find an entity with the specified id " + typeIdPair + " in the request");
               }
            }
        }

        context.removeUnResolvedIdReferences(resolvedReferences);

        //Resolve root references
        for (AtlasEntity entity : context.getRootEntities()) {
            if ( !context.isResolved(entity.getGuid()) && AtlasEntity.isAssigned(entity.getGuid())) {
                AtlasObjectId typeIdPair = new AtlasObjectId(entity.getTypeName(), entity.getGuid());
                Optional<AtlasVertex> vertex = resolveGuid(typeIdPair);
                if (vertex.isPresent()) {
                    context.addRepositoryResolvedReference(typeIdPair, vertex.get());
                    context.removeUnResolvedIdReference(typeIdPair);
                }
            }
        }
        return context;
    }

    private Optional<AtlasVertex> resolveGuid(AtlasObjectId typeIdPair) throws AtlasBaseException {
        //validate in graph repo that given guid, typename exists
        AtlasVertex vertex = null;
        try {
            vertex = graphHelper.findVertex(Constants.GUID_PROPERTY_KEY, typeIdPair.getGuid(),
                Constants.TYPE_NAME_PROPERTY_KEY, typeIdPair.getTypeName(),
                Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());
        } catch (EntityNotFoundException e) {
            //Ignore
        }
        if ( vertex != null ) {
            return Optional.of(vertex);
        } else {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, "Could not find an entity with the specified guid " + typeIdPair.getGuid() + " in Atlas respository");
        }
    }

    @Override
    public void cleanUp() throws AtlasBaseException {
        idToEntityMap.clear();
    }

}
