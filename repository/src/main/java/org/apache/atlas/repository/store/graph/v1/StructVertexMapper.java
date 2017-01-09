package org.apache.atlas.repository.store.graph.v1;


import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.Singleton;
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
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.typesystem.persistence.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class StructVertexMapper {

    private AtlasGraph graph;

    private GraphHelper graphHelper = GraphHelper.getInstance();

    private MapVertexMapper mapVertexMapper;

    private ArrayVertexMapper arrVertexMapper;

    private EntityVertexMapper entityVertexMapper;

    public StructVertexMapper(final AtlasGraph graph, final MapVertexMapper mapVertexMapper, final ArrayVertexMapper arrayVertexMapper) {
        this.graph = graph;
        this.mapVertexMapper = mapVertexMapper;
        this.arrVertexMapper = arrayVertexMapper;
    }

    @Inject
    public StructVertexMapper(AtlasGraph graph, MapVertexMapper mapVertexMapper, ArrayVertexMapper arrayVertexMapper, EntityVertexMapper entityVertexMapper) {
        this(graph, mapVertexMapper, arrayVertexMapper);
        this.entityVertexMapper = entityVertexMapper;
    }

    private static final Logger LOG = LoggerFactory.getLogger(StructVertexMapper.class);



    public Object toVertex(AtlasStructType parentType, AtlasStructDef.AtlasAttributeDef attributeDef, AtlasStructType attrType, Object value, AtlasVertex referringVertex, Optional<AtlasEdge> existingEdge) throws AtlasBaseException {
        AtlasEdge result = null;

        String edgeLabel = AtlasGraphUtilsV1.getAttributeEdgeLabel(parentType, attributeDef.getName());

        if ( existingEdge.isPresent() ) {
            updateVertex(parentType, attrType, attributeDef, (AtlasStruct) value, existingEdge.get().getOutVertex());
            result = existingEdge.get();
        } else {
            result = createVertex(parentType, attrType, attributeDef, (AtlasStruct) value, referringVertex, edgeLabel);
        }

        return result;
    }

    public static boolean shouldManageChildReferences(AtlasStructType type, String attributeName) {
        return type.isMappedFromRefAttribute(attributeName);
    }

    /**
     * Map attributes for entity, struct or trait
     * @param structType
     * @param struct
     * @param vertex
     * @return
     * @throws AtlasBaseException
     */
    public AtlasVertex mapAttributestoVertex(AtlasStructType structType, AtlasStruct struct, AtlasVertex vertex) throws AtlasBaseException {
        if (struct.getAttributes() != null) {
            for (String attrName : struct.getAttributes().keySet()) {
                Object value = struct.getAttribute(attrName);
                AtlasType attributeType = structType.getAttributeType(attrName);

                final AtlasStructDef.AtlasAttributeDef attributeDef = structType.getStructDef().getAttribute(attrName);

                mapToVertexByTypeCategory(structType, attributeDef, attributeType, value, vertex, AtlasGraphUtilsV1.getQualifiedAttributePropertyKey(structType, attrName), Optional.<AtlasEdge>absent());
            }
        }
        return vertex;
    }

    protected Object mapToVertexByTypeCategory(AtlasType parentType, AtlasStructDef.AtlasAttributeDef attributeDef, AtlasType attrType, Object value, AtlasVertex vertex, String vertexPropertyName, Optional<AtlasEdge> existingEdge) throws AtlasBaseException {
        switch(attrType.getTypeCategory()) {
        case PRIMITIVE:
        case ENUM:
            return primitivesToVertex(parentType, attributeDef, attrType, value, vertex, vertexPropertyName);
        case STRUCT:
            return toVertex((AtlasStructType) parentType, attributeDef, (AtlasStructType) attrType, value, vertex, existingEdge);
        case ENTITY:
            return entityVertexMapper.toVertex((AtlasEntityType) parentType, attributeDef, (AtlasEntityType) attrType, value, vertex, existingEdge);
        case MAP:
            return mapVertexMapper.toVertex((AtlasStructType) parentType, attributeDef, (AtlasMapType) attrType, value, vertex, vertexPropertyName);
        case ARRAY:
            return arrVertexMapper.toVertex((AtlasStructType) parentType, attributeDef, (AtlasArrayType) attrType, value, vertex, vertexPropertyName);
        default:
            throw new AtlasBaseException(AtlasErrorCode.TYPE_CATEGORY_INVALID, attrType.getTypeCategory().name());
        }
    }

    protected Object primitivesToVertex(AtlasType parentType, AtlasStructDef.AtlasAttributeDef attributeDef, AtlasType attrType, Object val, AtlasVertex vertex, String vertexPropertyName) {
        if ( parentType.getTypeCategory() == TypeCategory.MAP ) {
            MapVertexMapper.setMapValueProperty(((AtlasMapType)parentType).getValueType(), vertex, vertexPropertyName, val);
        } else {
            AtlasGraphUtilsV1.setProperty(vertex, vertexPropertyName, val);
        }
        return val;
    }

    private AtlasEdge createVertex(AtlasStructType parentType, AtlasStructType structAttributeType, AtlasStructDef.AtlasAttributeDef attributeDef, AtlasStruct struct, AtlasVertex referringVertex, String edgeLabel) throws AtlasBaseException {
        AtlasVertex vertex = createVertexTemplate(struct, structAttributeType);
        mapAttributestoVertex(structAttributeType, struct, vertex);

        try {
            return graphHelper.getOrCreateEdge(referringVertex, vertex, edgeLabel);
        } catch (RepositoryException e) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }
    }

    private void updateVertex(AtlasStructType parentType, AtlasStructType structAttributeType, AtlasStructDef.AtlasAttributeDef attributeDef, AtlasStruct value, AtlasVertex structVertex) throws AtlasBaseException {
        mapAttributestoVertex(structAttributeType, value, structVertex);
    }

    protected AtlasVertex createVertexTemplate(final AtlasStruct instance, final AtlasStructType structType) {
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

    protected Object mapCollectionElementsToVertex(AtlasType parentType, AtlasStructDef.AtlasAttributeDef attributeDef, AtlasType attrType, Object value, AtlasVertex vertex, String vertexPropertyName, Optional<AtlasEdge> existingEdge) throws AtlasBaseException {
        switch(attrType.getTypeCategory()) {
        case PRIMITIVE:
        case ENUM:
            return primitivesToVertex(parentType, attributeDef, attrType, value, vertex, vertexPropertyName);
        case STRUCT:
            return toVertex((AtlasStructType) parentType, attributeDef, (AtlasStructType) attrType, value, vertex, existingEdge);
        case ENTITY:
            return entityVertexMapper.toVertex((AtlasEntityType) parentType, attributeDef, (AtlasEntityType) attrType, value, vertex, existingEdge);
        case MAP:
        case ARRAY:
        default:
            throw new AtlasBaseException(AtlasErrorCode.TYPE_CATEGORY_INVALID, attrType.getTypeCategory().name());
        }
    }
}
