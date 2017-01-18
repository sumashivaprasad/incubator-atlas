package org.apache.atlas.repository.store.graph.v1;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
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

public class StructVertexMapper implements InstanceGraphMapper<AtlasEdge> {

    private final AtlasGraph graph;

    private final GraphHelper graphHelper = GraphHelper.getInstance();

    private MapVertexMapper mapVertexMapper;

    private ArrayVertexMapper arrVertexMapper;

    private EntityGraphMapper entityVertexMapper;

    private static final Logger LOG = LoggerFactory.getLogger(StructVertexMapper.class);

    public StructVertexMapper(ArrayVertexMapper arrayVertexMapper, MapVertexMapper mapVertexMapper) {
        this.graph = AtlasGraphProvider.getGraphInstance();;
        this.mapVertexMapper = mapVertexMapper;
        this.arrVertexMapper = arrayVertexMapper;
    }

    void init(final EntityGraphMapper entityVertexMapper) {
        this.entityVertexMapper = entityVertexMapper;
    }

    public AtlasEdge toGraph(GraphMutationContext ctx) throws AtlasBaseException {
        AtlasEdge result = null;

        String edgeLabel = AtlasGraphUtilsV1.getAttributeEdgeLabel(ctx.getParentType(), ctx.getAttributeDef().getName());

        if ( ctx.getCurrentEdge().isPresent() ) {
            updateVertex(ctx.getParentType(), (AtlasStructType) ctx.getAttrType(), ctx.getAttributeDef(), (AtlasStruct) ctx.getValue(), ctx.getCurrentEdge().get().getOutVertex());
            result = ctx.getCurrentEdge().get();
        } else {
            result = createVertex(ctx.getParentType(), (AtlasStructType) ctx.getAttrType(), ctx.getAttributeDef(), (AtlasStruct) ctx.getValue(), ctx.getReferringVertex(), edgeLabel);
        }

        return result;
    }

    @Override
    public void cleanUp() throws AtlasBaseException {
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
                if ( attributeType != null) {
                    final AtlasStructDef.AtlasAttributeDef attributeDef = structType.getAttribute(attrName);
                    GraphMutationContext ctx =  new GraphMutationContext.Builder(structType, attributeDef, attributeType, value)
                        .referringVertex(vertex)
                        .vertexProperty(AtlasGraphUtilsV1.getQualifiedAttributePropertyKey(structType, attrName)).build();
                    mapToVertexByTypeCategory(ctx);
                }
            }
        }
        return vertex;
    }

    protected Object mapToVertexByTypeCategory(GraphMutationContext ctx) throws AtlasBaseException {
        switch(ctx.getAttrType().getTypeCategory()) {
        case PRIMITIVE:
        case ENUM:
            return primitivesToVertex(ctx);
        case STRUCT:
            return toGraph(ctx);
        case ENTITY:
            AtlasEntityType instanceType = entityVertexMapper.getInstanceType(ctx.getValue());
            ctx.setAttrType(instanceType);
            return entityVertexMapper.toGraph(ctx);
        case MAP:
            return mapVertexMapper.toGraph(ctx);
        case ARRAY:
            return arrVertexMapper.toGraph(ctx);
        default:
            throw new AtlasBaseException(AtlasErrorCode.TYPE_CATEGORY_INVALID, ctx.getAttrType().getTypeCategory().name());
        }
    }

    protected Object primitivesToVertex(GraphMutationContext ctx) {
        if ( ctx.getAttrType().getTypeCategory() == TypeCategory.MAP ) {
            MapVertexMapper.setMapValueProperty(((AtlasMapType) ctx.getAttrType()).getValueType(), ctx.getReferringVertex(), ctx.getVertexPropertyKey(), ctx.getValue());
        } else {
            AtlasGraphUtilsV1.setProperty(ctx.getReferringVertex(), ctx.getVertexPropertyKey(), ctx.getValue());
        }
        return ctx.getValue();
    }

    private AtlasEdge createVertex(AtlasStructType parentType, AtlasStructType attrType, AtlasStructDef.AtlasAttributeDef attributeDef, AtlasStruct struct, AtlasVertex referringVertex, String edgeLabel) throws AtlasBaseException {
        AtlasVertex vertex = createVertexTemplate(struct, attrType);
        mapAttributestoVertex(attrType, struct, vertex);

        try {
            //TODO - Map directly in AtlasGraphUtilsV1
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
        AtlasGraphUtilsV1.setProperty(vertexWithoutIdentity, Constants.TIMESTAMP_PROPERTY_KEY, RequestContextV1.get().getRequestTime());
        AtlasGraphUtilsV1.setProperty(vertexWithoutIdentity, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY,
            RequestContextV1.get().getRequestTime());

        return vertexWithoutIdentity;
    }

    protected Object mapCollectionElementsToVertex(GraphMutationContext ctx) throws AtlasBaseException {
        switch(ctx.getAttrType().getTypeCategory()) {
        case PRIMITIVE:
        case ENUM:
            return primitivesToVertex(ctx);
        case STRUCT:
            return toGraph(ctx);
        case ENTITY:
            AtlasEntityType instanceType = entityVertexMapper.getInstanceType(ctx.getValue());
            ctx.setAttrType(instanceType);
            return entityVertexMapper.toGraph(ctx);
        case MAP:
        case ARRAY:
        default:
            throw new AtlasBaseException(AtlasErrorCode.TYPE_CATEGORY_INVALID, ctx.getAttrType().getTypeCategory().name());
        }
    }
}
