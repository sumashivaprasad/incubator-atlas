package org.apache.atlas.repository.store.graph.v1;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.DiscoveredEntities;
import org.apache.atlas.repository.store.graph.EntityResolver;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.persistence.Id;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IDBasedEntityResolver implements EntityResolver {

    private Map<Id, AtlasEntity> idToEntityMap = new HashMap<>();

    private GraphHelper graphHelper = GraphHelper.getInstance();

    public DiscoveredEntities resolveEntityReferences(DiscoveredEntities entities) throws EntityNotFoundException, AtlasBaseException {
        final List<AtlasEntity> rootReferences = entities.getRootEntities();
        for (AtlasEntity entity : rootReferences) {
            idToEntityMap.put(new Id(entity.getTypeName(), 0, entity.getGuid()), entity);
        }

        for (AtlasEntity entity : entities.getUnResolvedReferences()) {
            if (entity.isAssigned()) {
                //validate in graph repo that given guid, typename exists
                AtlasVertex vertex =  graphHelper.findVertex(entity.getGuid(), entity.getTypeName());
                if ( vertex != null ) {
                    entities.removeUnResolvedReference(entity);
                    entities.addResolvedReference(entity, vertex);
                } else {
                    throw new AtlasBaseException("Could not find an entity with the specified guid " + entity.getGuid() + " in Atlas ");
                }

            } else {
                //check if root references have this temporary id
                idToEntityMap.get(new Id(entity.getTypeName(), 0, entity.getGuid()));
            }
        }

        return entities;
    }



}
