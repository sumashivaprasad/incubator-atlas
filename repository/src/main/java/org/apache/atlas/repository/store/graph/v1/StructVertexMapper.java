package org.apache.atlas.repository.store.graph.v1;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.typesystem.persistence.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class StructVertexMapper {

    private AtlasGraph graph;

    private GraphHelper graphHelper;

    private static final Logger LOG = LoggerFactory.getLogger(StructVertexMapper.class);

    public AtlasVertex createVertexTemplate(final AtlasStruct instance, final AtlasStructType structType) {
        LOG.debug("Creating AtlasVertex for type {}", instance.getTypeName());
        final AtlasVertex vertexWithoutIdentity = graph.addVertex();

        // add type information
        AtlasGraphUtilsV1.setProperty(vertexWithoutIdentity, Constants.ENTITY_TYPE_PROPERTY_KEY, instance.getTypeName());

        // add state information
        AtlasGraphUtilsV1.setProperty(vertexWithoutIdentity, Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());

        // add timestamp information
        AtlasGraphUtilsV1.setProperty(vertexWithoutIdentity, Constants.TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
        AtlasGraphUtilsV1.setProperty(vertexWithoutIdentity, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY,
            RequestContext.get().getRequestTime());

        return vertexWithoutIdentity;
    }

    public AtlasVertex mapByCategory(AtlasStruct struct, AtlasVertex vertex, AtlasStructType structType, Set<TypeCategory> typeCategories) {

        if (struct.getAttributes() != null) {
            for (String attrName : struct.getAttributes().keySet()) {
                Object value = struct.getAttribute(attrName);

                AtlasType type = structType.getAttributeType(attrName);
                if (typeCategories.contains(type.getTypeCategory())) {
                    AtlasGraphUtilsV1.setProperty(vertex, attrName, value);
                }
            }
        }
        return vertex;
    }

    public AtlasVertex mapAttributestoVertex(AtlasStruct struct, AtlasVertex vertex, AtlasStructType structType) throws AtlasBaseException {

        if (struct.getAttributes() != null) {
            for (String attrName : struct.getAttributes().keySet()) {
                Object value = struct.getAttribute(attrName);
                AtlasType type = structType.getAttributeType(attrName);

                switch(type.getTypeCategory()) {
                case PRIMITIVE:
                case ENUM:
                    mapPrimitives(vertex, value, type);
                    break;
                case STRUCT:
                    String edgeLabel = AtlasGraphUtilsV1.getAttributeEdgeLabel(structType, attrName);
                    Iterator<AtlasEdge> currentEdge = graphHelper.getOutGoingEdgesByLabel(vertex, edgeLabel);

                    if ( currentEdge.hasNext() ) {
                        updateVertex(currentEdge.next().getOutVertex(), (AtlasStruct) value, (AtlasStructType) type);
                    } else {
                        createVertex(vertex, (AtlasStruct) value, (AtlasStructType) type, edgeLabel);
                    }
                    break;
                case MAP:

                }
            }
        }
        return vertex;
    }

    private AtlasVertex mapPrimitives(AtlasVertex vertex, Object val, AtlasType type) {
        AtlasGraphUtilsV1.setProperty(vertex, type.getTypeName(), val);
        return vertex;
    }

    public AtlasEdge createVertex(AtlasVertex referringVertex, AtlasStruct struct, AtlasStructType structAttributeType, String edgeLabel) throws AtlasBaseException {
        AtlasVertex vertex = createVertexTemplate(struct, structAttributeType);
        mapAttributestoVertex(struct, vertex, structAttributeType);

        try {
            return graphHelper.getOrCreateEdge(referringVertex, vertex, edgeLabel);
        } catch (RepositoryException e) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }
    }


    public void updateVertex(AtlasVertex referringVertex, AtlasStruct struct, AtlasStructType structAttributeType) throws AtlasBaseException {
        mapAttributestoVertex(struct, referringVertex, structAttributeType);
    }


}
