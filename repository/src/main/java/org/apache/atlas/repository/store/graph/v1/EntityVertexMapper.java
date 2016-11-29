package org.apache.atlas.repository.store.graph.v1;


import com.google.common.base.Optional;
import com.google.inject.Singleton;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.text.html.Option;
import java.util.Iterator;
import java.util.UUID;

import static org.apache.atlas.repository.graph.GraphHelper.string;

public class EntityVertexMapper extends StructVertexMapper {

    private static final Logger LOG = LoggerFactory.getLogger(EntityVertexMapper.class);

    private GraphHelper graphHelper = GraphHelper.getInstance();

    EntityGraphDiscoveryContext context;

    public EntityVertexMapper(EntityGraphDiscoveryContext context) {
        this.context = context;
    }

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

        AtlasGraphUtilsV1.setProperty(vertex, Constants.CREATED_BY_KEY, RequestContext.get().getUser());

        GraphHelper.setProperty(vertex, Constants.MODIFIED_BY_KEY, RequestContext.get().getUser());

        AtlasGraphUtilsV1.setProperty(vertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContext.get().getUser());

        return vertex;
    }


    public AtlasEdge updateEdge(AtlasStructType parentType, AtlasStructType structAttributeType, AtlasStructDef.AtlasAttributeDef attributeDef, Object value, AtlasEdge existingEdge, final AtlasVertex entityVertex) throws AtlasBaseException {

        LOG.debug("Updating entity reference {} for reference attribute {}",  attributeDef.getName());
        // Update edge if it exists

        AtlasVertex currentVertex = existingEdge.getOutVertex();
        String currentEntityId = GraphHelper.getIdFromVertex(currentVertex);
        String newEntityId = getId(value);
        AtlasEdge newEdge = existingEdge;
        if (!currentEntityId.equals(newEntityId)) {
            // add an edge to the class vertex from the instance
            if (entityVertex != null) {
                try {
                    newEdge = graphHelper.getOrCreateEdge(existingEdge.getInVertex(), entityVertex, existingEdge.getLabel());
                } catch (RepositoryException e) {
                    throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
                }

            }
        }
        return newEdge;
    }

    @Override
    public Object createOrUpdate(AtlasStructType parentType, AtlasStructDef.AtlasAttributeDef attributeDef, AtlasStructType attrType, Object value, AtlasVertex referringVertex, Optional<AtlasEdge> existingEdge) throws AtlasBaseException {
        AtlasEdge result = null;

        AtlasVertex entityVertex = context.getResolvedReference(new AtlasObjectId(attrType.getTypeName(), (String) value));
        String edgeLabel = AtlasGraphUtilsV1.getAttributeEdgeLabel(parentType, attributeDef.getName());
        if ( existingEdge.isPresent() ) {
            updateEdge(parentType, attrType, attributeDef, value, existingEdge.get(), entityVertex);
            result = existingEdge.get();
        } else {
            try {
                result = graphHelper.getOrCreateEdge(referringVertex, entityVertex, edgeLabel);
            } catch (RepositoryException e) {
                throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
            }
        }

        return result;
    }

    String getId(Object value) throws AtlasBaseException {
        if ( value != null) {
            if (value instanceof String) {
                return (String) value;
            } else {
                return ((AtlasEntity) value).getGuid();
            }
        }
        throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, (String) value);
    }
}
