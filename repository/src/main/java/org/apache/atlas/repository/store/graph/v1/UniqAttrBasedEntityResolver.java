package org.apache.atlas.repository.store.graph.v1;

import com.google.common.base.Optional;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.repository.store.graph.EntityResolver;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public class UniqAttrBasedEntityResolver implements EntityResolver {

    private static final Logger LOG = LoggerFactory.getLogger(UniqAttrBasedEntityResolver.class);

    private AtlasTypeRegistry typeRegistry;

    private GraphHelper graphHelper = GraphHelper.getInstance();

    @Inject
    public UniqAttrBasedEntityResolver(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Override
    public EntityGraphDiscoveryContext resolveEntityReferences(EntityGraphDiscoveryContext entities) throws AtlasBaseException {

        //Resolve attribute references
        List<AtlasEntity> resolvedReferences = new ArrayList<>();

        for (AtlasEntity entity : entities.getUnResolvedEntityReferences()) {
            //query in graph repo that given unique attribute - check for deleted also?
            Optional<AtlasVertex> vertex = resolveByUniqueAttribute(entity);
            if (vertex.isPresent()) {
                entities.addRepositoryResolvedReference(entity, vertex.get());
                resolvedReferences.add(entity);
            }
        }

        entities.removeUnResolvedEntityReferences(resolvedReferences);

        if (entities.getUnResolvedEntityReferences().size() > 0) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, entities.getUnResolvedEntityReferences().toString());
        }

        //Resolve root references
        for (AtlasEntity entity : entities.getRootEntities()) {
            if ( !entities.isResolved(entity) ) {
                Optional<AtlasVertex> vertex = resolveByUniqueAttribute(entity);
                if (vertex.isPresent()) {
                    entities.addRepositoryResolvedReference(entity, vertex.get());
                }
            }
        }

        return entities;
    }

    Optional<AtlasVertex> resolveByUniqueAttribute(AtlasEntity entity) throws AtlasBaseException {
        AtlasEntityType entityType = (AtlasEntityType) typeRegistry.getType(entity.getTypeName());
        for (AtlasStructDef.AtlasAttributeDef attrDef : entityType.getAllAttributeDefs().values()) {
            if (attrDef.getIsUnique()) {
                Object attrVal = entity.getAttribute(attrDef.getName());
                if (attrVal != null) {
                    String qualifiedAttrName = entityType.getQualifiedAttributeName(attrDef.getName());
                    AtlasVertex vertex = null;
                    try {
                        vertex = graphHelper.findVertex(qualifiedAttrName, attrVal,
                            Constants.ENTITY_TYPE_PROPERTY_KEY, entityType.getTypeName(),
                            Constants.STATE_PROPERTY_KEY, AtlasEntity.Status.STATUS_ACTIVE.name());
                        LOG.debug("Found vertex by unique attribute : " + qualifiedAttrName + "=" + attrVal);
                        if (vertex != null) {
                            return Optional.of(vertex);
                        }
                    } catch (EntityNotFoundException e) {
                        //Ignore if not found
                    }
                }
            }
        }
        return Optional.absent();
    }
}

