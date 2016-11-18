package org.apache.atlas.repository.store.graph.v1;


import com.google.inject.Singleton;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.UUID;

import static org.apache.atlas.repository.graph.GraphHelper.string;

@Singleton
public class EntityVertexMapper extends StructVertexMapper {

    private static final Logger LOG = LoggerFactory.getLogger(EntityVertexMapper.class);

    private GraphHelper graphHelper;

    @Override
    public AtlasVertex createVertexTemplate(final AtlasStruct instance, final AtlasStructType structType) {
        AtlasVertex vertex = createVertexTemplate(instance, structType);
        
        // add super types
        AtlasEntityType entityType = (AtlasEntityType) structType;

        AtlasEntity entity = (AtlasEntity) instance;

        for (String superTypeName : entityType.getAllSuperTypes()) {
            AtlasGraphUtilsV1.addProperty(vertex, Constants.SUPER_TYPES_PROPERTY_KEY, superTypeName);
        }

        final String guid = UUID.randomUUID().toString();

        // add identity
        AtlasGraphUtilsV1.setProperty(vertex, Constants.GUID_PROPERTY_KEY, guid);

        // add version information
        AtlasGraphUtilsV1.setProperty(vertex, Constants.VERSION_PROPERTY_KEY, entity.getVersion());

        //TODO - Set system properties like createdBy, createdTime etc
        return vertex;
    }


    public void updateEdge(AtlasStructType parentType, AtlasStructType structAttributeType, AtlasStructDef.AtlasAttributeDef attributeDef, Object value, AtlasEdge existingEdge) throws AtlasBaseException {

        LOG.debug("Updating entity reference {} for reference attribute {}",  attributeDef.getName());
        // Update edge if it exists

        AtlasVertex currentVertex = existingEdge.getOutVertex();
        String currentEntityId = GraphHelper.getIdFromVertex(currentVertex);
        String newEntityId = getId(value);
        AtlasEdge newEdge = existingEdge;
        if (!currentEntityId.equals(newEntityId)) {
            // add an edge to the class vertex from the instance
            if (newVertex != null) {
                newEdge = graphHelper.getOrCreateEdge(instanceVertex, newVertex, edgeLabel);

            }
        }

        return newEdge;
    }

    @Override
    public Object createOrUpdate(AtlasStructType parentType, AtlasStructDef.AtlasAttributeDef attributeDef, AtlasStructType attrType, Object value, AtlasVertex referringVertex) throws AtlasBaseException {
        AtlasEdge result = null;

        AtlasVertex newReferenceVertex = context.getResolvedReference(newAttributeValue);


        String edgeLabel = AtlasGraphUtilsV1.getAttributeEdgeLabel(parentType, attributeDef.getName());
        Iterator<AtlasEdge> currentEdgeIter = graphHelper.getOutGoingEdgesByLabel(referringVertex, edgeLabel);

        if ( currentEdgeIter.hasNext() ) {
            AtlasEdge existingEdge = currentEdgeIter.next();
            updateEdge(parentType, attrType, attributeDef, value, existingEdge);
            result = existingEdge;
        } else {
            result = createEdge(parentType, attrType, attributeDef, (AtlasStruct) value, referringVertex, edgeLabel);
        }

        return result;
    }
}
