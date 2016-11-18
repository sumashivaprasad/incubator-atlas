package org.apache.atlas.repository.store.graph.v1;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasMapType;
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

    private MapVertexMapper mapVertexMapper;

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

//    public AtlasVertex mapByCategory(AtlasStruct struct, AtlasVertex vertex, AtlasStructType structType, Set<TypeCategory> typeCategories) {
//
//        if (struct.getAttributes() != null) {
//            for (String attrName : struct.getAttributes().keySet()) {
//                Object value = struct.getAttribute(attrName);
//
//                AtlasType type = structType.getAttributeType(attrName);
//                if (typeCategories.contains(type.getTypeCategory())) {
//                    AtlasGraphUtilsV1.setProperty(vertex, attrName, value);
//                }
//            }
//        }
//        return vertex;
//    }

    public AtlasVertex mapAttributestoVertex(AtlasStructType structType, AtlasStruct struct, AtlasVertex vertex) throws AtlasBaseException {

        if (struct.getAttributes() != null) {
            for (String attrName : struct.getAttributes().keySet()) {
                Object value = struct.getAttribute(attrName);
                AtlasType attributeType = structType.getAttributeType(attrName);

                final AtlasStructDef.AtlasAttributeDef attributeDef = structType.getStructDef().getAttribute(attrName);

                mapToVertexByTypeCategory(structType, attributeDef, attributeType, value, vertex);


            }
        }
        return vertex;
    }

    public Object mapToVertexByTypeCategory(AtlasType parentType, AtlasStructDef.AtlasAttributeDef attributeDef, AtlasType attrType, Object value, AtlasVertex vertex) throws AtlasBaseException {
        switch(attrType.getTypeCategory()) {
        case PRIMITIVE:
        case ENUM:
            return createOrUpdatePrimitives(parentType, attributeDef, attrType, value, vertex);
        case STRUCT:
            return createOrUpdate((AtlasStructType) parentType, attributeDef, (AtlasStructType) attrType, value, vertex);
        case MAP:
            return mapVertexMapper.mapToVertex((AtlasStructType) parentType, attributeDef, (AtlasMapType) attrType, value, vertex);
        default:
            throw new AtlasBaseException(AtlasErrorCode.TYPE_CATEGORY_INVALID, attrType.getTypeCategory().name());
        }

    }

    public Object createOrUpdatePrimitives(AtlasType parentType, AtlasStructDef.AtlasAttributeDef attributeDef, AtlasType attrType, Object val, AtlasVertex vertex) {
        AtlasGraphUtilsV1.setProperty(vertex, parentType.getTypeName(), val);
        return val;
    }

    public AtlasEdge createVertex(AtlasVertex referringVertex, AtlasStruct struct, AtlasStructType structAttributeType, String edgeLabel) throws AtlasBaseException {
        AtlasVertex vertex = createVertexTemplate(struct, structAttributeType);
        mapAttributestoVertex(structAttributeType, struct, vertex);

        try {
            return graphHelper.getOrCreateEdge(referringVertex, vertex, edgeLabel);
        } catch (RepositoryException e) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }
    }

    public void updateVertex(AtlasStruct struct, AtlasStructType structAttributeType, AtlasVertex structVertex) throws AtlasBaseException {
        mapAttributestoVertex(structAttributeType, struct, structVertex);
    }

    public Object createOrUpdate(AtlasStructType parentType, AtlasStructDef.AtlasAttributeDef attributeDef, AtlasStructType attrType, Object value, AtlasVertex referringVertex) throws AtlasBaseException {
        AtlasEdge result = null;

        String edgeLabel = AtlasGraphUtilsV1.getAttributeEdgeLabel(parentType, attributeDef.getName());
        Iterator<AtlasEdge> currentEdgeIter = graphHelper.getOutGoingEdgesByLabel(referringVertex, edgeLabel);

        if ( currentEdgeIter.hasNext() ) {
            AtlasEdge existingEdge = currentEdgeIter.next();
            updateVertex((AtlasStruct) value, attrType, existingEdge.getOutVertex());
            result = existingEdge;
        } else {
            result = createVertex(referringVertex, (AtlasStruct) value, attrType, edgeLabel);
        }

        return result;
    }

    public static boolean shouldManageChildReferences(AtlasStructDef.AtlasAttributeDef attributeDef) {
        return attributeDef.getConstraintDefs().contains(AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_MAPPED_FROM_REF);
    }
}
