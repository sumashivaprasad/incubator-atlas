package org.apache.atlas.repository.store.graph.v1;

import com.google.common.base.Optional;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.DiscoveredEntities;
import org.apache.atlas.repository.store.graph.EntityResolver;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.swing.text.html.Option;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class UniqAttrBasedEntityResolver implements EntityResolver {

    private static final Logger LOG = LoggerFactory.getLogger(UniqAttrBasedEntityResolver.class);
//    private Map<TypeAttributeTuple, AtlasEntity> uniqAttributeToEntityMap = new LinkedHashMap<>();

    private AtlasTypeRegistry typeRegistry;

    private GraphHelper graphHelper = GraphHelper.getInstance();

//    private class TypeAttributeTuple {
//        public final String typeName;
//        public final String attrName;
//        public final String attrVal;
//
//        public TypeAttributeTuple(String typeName, String attrName, String attrVal) {
//            this.typeName = typeName;
//            this.attrName = attrName;
//            this.attrVal = attrVal;
//        }
//    }

    @Inject
    public UniqAttrBasedEntityResolver(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Override
    public DiscoveredEntities resolveEntityReferences(DiscoveredEntities entities) throws AtlasBaseException {
//        addToUniqueList(entities.getRootEntities());
//        addToUniqueList(entities.getUnResolvedReferences());

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
            //TODO - format error
            throw new AtlasBaseException("Could not find an entity with the specified entity references " + entities.getUnResolvedEntityReferences() + " in Atlas ");
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

    private void addToUniqueList(List<AtlasEntity> rootReferences) throws AtlasBaseException {

//        Predicate<AtlasStructDef.AtlasAttributeDef> filterUnique = new Predicate<AtlasStructDef.AtlasAttributeDef>() {
//            public boolean apply(AtlasStructDef.AtlasAttributeDef attributeDef) {
//                AtlasType attrType = typeRegistry.getType(attributeDef.getTypeName());
//                if ( attributeDef.getIsUnique() && attrType.getTypeCategory() == TypeCategory.PRIMITIVE) {
//                    return true;
//                }
//                return false;
//            }
//        };

//        for (AtlasEntity entity : rootReferences) {
//            AtlasEntityType typeDef = (AtlasEntityType) typeRegistry.getType(entity.getTypeName());
//            for (AtlasStructDef.AtlasAttributeDef attrDef : typeDef.getAllAttributeDefs().values()) {
//                if (attrDef.getIsUnique() && typeDef.getAttributeType(attrDef.getName()).getTypeCategory() == TypeCategory.PRIMITIVE) {
//                    Object attrVal = entity.getAttribute(attrDef.getName());
//                    if (attrVal != null) {
//                        uniqAttributeToEntityMap.put(new TypeAttributeTuple(entity.getTypeName(), attrDef.getName(), String.valueOf(attrVal)), entity);
//                    }
//                }
//            }
//            final UnmodifiableIterator<AtlasStructDef.AtlasAttributeDef> iter = Iterators.filter(typeDef.get.iterator(), filterUnique);

//            while(iter.hasNext()) {
//                AtlasStructDef.AtlasAttributeDef attrDef = iter.next();
//                Object attrVal = entity.getAttribute(attrDef.getTypeName());
//                if ( attrVal != null) {
//                    uniqAttributeToEntityMap.put(new TypeAttributeTuple(entity.getTypeName(), attrDef.getName(), String.valueOf(attrVal)), entity);
//                }
//            }
//        }
    }

}

