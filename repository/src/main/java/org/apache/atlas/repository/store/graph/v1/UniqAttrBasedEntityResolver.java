package org.apache.atlas.repository.store.graph.v1;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.DiscoveredEntities;
import org.apache.atlas.repository.store.graph.EntityResolver;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;

import javax.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class UniqAttrBasedEntityResolver implements EntityResolver {

    private Map<TypeAttributeTuple, AtlasEntity> uniqAttributeToEntityMap = new LinkedHashMap<>();

    private AtlasTypeRegistry typeRegistry;

    private GraphHelper graphHelper = GraphHelper.getInstance();

    private class TypeAttributeTuple {
        public final String typeName;
        public final String attrName;
        public final String attrVal;

        public TypeAttributeTuple(String typeName, String attrName, String attrVal) {
            this.typeName = typeName;
            this.attrName = attrName;
            this.attrVal = attrVal;
        }
    }

    @Inject
    public UniqAttrBasedEntityResolver(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Override
    public DiscoveredEntities resolveEntityReferences(DiscoveredEntities entities) throws EntityNotFoundException, AtlasBaseException {
        addToUniqueList(entities.getRootEntities());
        addToUniqueList(entities.getUnResolvedReferences());

        for (AtlasEntity entity : entities.getUnResolvedReferences()) {
            if (!entity.isAssigned()) {
                //query in graph repo that given unique attribute - check for deleted also?
                AtlasVertex vertex =  graphHelper.findVertex(entity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME), entity.getTypeName());
                if ( vertex != null ) {
                    entities.removeUnResolvedReference(entity);
                    entities.addResolvedReference(entity, vertex);
                } else {
                    throw new AtlasBaseException("Could not find an entity with the specified guid " + entity.getGuid() + " in Atlas ");
                }
            }
        }
        return entities;
    }

    private void addToUniqueList(List<AtlasEntity> rootReferences) {

        Predicate<AtlasStructDef.AtlasAttributeDef> filterUnique = new Predicate<AtlasStructDef.AtlasAttributeDef>() {
            public boolean apply(AtlasStructDef.AtlasAttributeDef attributeDef) {
                AtlasType attrType = typeRegistry.getType(attributeDef.getTypeName());
                if ( attributeDef.isUnique() && attrType.getTypeCategory() == AtlasType.TypeCategory.PRIMITIVE) {
                    return true;
                }
                return false;
            }
        };

        for (AtlasEntity entity : rootReferences) {
            AtlasStructType typeDef = (AtlasStructType) typeRegistry.getType(entity.getTypeName());
            final UnmodifiableIterator<AtlasStructDef.AtlasAttributeDef> iter = Iterators.filter(typeDef.getStructDefinition().getAttributeDefs().iterator(), filterUnique);

            while(iter.hasNext()) {
                AtlasStructDef.AtlasAttributeDef attrDef = iter.next();
                Object attrVal = entity.getAttribute(attrDef.getTypeName());
                if ( attrVal != null) {
                    uniqAttributeToEntityMap.put(new TypeAttributeTuple(entity.getTypeName(), String.valueOf(attrVal)), entity);
                }
            }
        }
    }

}

