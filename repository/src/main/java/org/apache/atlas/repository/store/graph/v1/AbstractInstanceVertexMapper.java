package org.apache.atlas.repository.store.graph.v1;


import org.apache.atlas.RequestContext;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.typesystem.persistence.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractInstanceVertexMapper {

    private AtlasGraph graph;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractInstanceVertexMapper.class);

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

    public AtlasVertex addPrimitiveAttributes(AtlasVertex vertex, AtlasStruct struct, AtlasStructType structType) {

        if (struct.getAttributes() != null) {
            for (String attrName : struct.getAttributes().keySet()) {
                Object value = struct.getAttribute(attrName);

                AtlasType type = structType.getAttributeType(attrName);
                if (type.getTypeCategory() == TypeCategory.ENUM ||
                    type.getTypeCategory() == TypeCategory.PRIMITIVE) {
                    AtlasGraphUtilsV1.setProperty(vertex, attrName, value);
                }
            }
        }
        return vertex;
    }
}
