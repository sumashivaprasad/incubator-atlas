package org.apache.atlas.repository.graph;


import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UniqueKeyDedupHandler implements EntityDedupHandler {

    private static final GraphHelper graphHelper = GraphHelper.getInstance();

    private static final Logger LOG = LoggerFactory.getLogger(UniqueKeyDedupHandler.class);
    private Vertex instanceVertex;

    @Override
    public boolean exists(final ClassType classType, final IReferenceableInstance instance) throws AtlasException {
        LOG.debug("Checking if there is an instance with the same unique attributes for instance {}", instance.toShortString());
        for (AttributeInfo attributeInfo : classType.fieldMapping().fields.values()) {
            if (attributeInfo.isUnique) {
                String propertyKey = graphHelper.getQualifiedFieldName(classType, attributeInfo.name);
                try {
                    instanceVertex = graphHelper.getVertexForProperty(propertyKey, instance.get(attributeInfo.name));
                    LOG.debug("Found vertex by unique attribute : " + propertyKey + "=" + instance.get(attributeInfo.name));
                } catch (EntityNotFoundException e) {
                    //Its ok if there is no entity with the same unique value
                }
            }
        }
        return instanceVertex != null;
    }

    @Override
    public Vertex vertex(final ClassType classType, final IReferenceableInstance instance) throws AtlasException {
        if (instanceVertex == null) {
            exists(classType, instance);
        }
        return instanceVertex;
    }
}
