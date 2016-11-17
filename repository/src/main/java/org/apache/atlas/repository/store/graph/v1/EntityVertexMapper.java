package org.apache.atlas.repository.store.graph.v1;


import com.google.inject.Singleton;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;

import java.util.UUID;

@Singleton
public class EntityVertexMapper extends AbstractInstanceVertexMapper {

    public AtlasVertex createVertex(AtlasEntity entity, AtlasEntityType type) {
        AtlasEntityType entityType = (AtlasEntityType) type;
        AtlasVertex vertex = createVertexTemplate(entity, entityType);
        
        // add super types
        for (String superTypeName : entityType.getAllSuperTypes()) {
            AtlasGraphUtilsV1.addProperty(vertex, Constants.SUPER_TYPES_PROPERTY_KEY, superTypeName);
        }

        final String guid = UUID.randomUUID().toString();

        // add identity
        AtlasGraphUtilsV1.setProperty(vertex, Constants.GUID_PROPERTY_KEY, guid);

        // add version information
        AtlasGraphUtilsV1.setProperty(vertex, Constants.VERSION_PROPERTY_KEY, entity.getVersion());


        
        return vertex;
    }
}
