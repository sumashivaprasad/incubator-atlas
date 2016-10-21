package org.apache.atlas.repository.store.graph.v1;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.sun.xml.bind.IDResolver;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.DiscoveredEntities;
import org.apache.atlas.repository.store.graph.EntityGraphDiscovery;
import org.apache.atlas.repository.store.graph.EntityResolver;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class AtlasEntityGraphDiscoveryV1 implements EntityGraphDiscovery {

    private AtlasTypeRegistry typeRegistry;

    List<String> processedIds = new ArrayList<>();

    DiscoveredEntities discoveredEntities = new DiscoveredEntities();

    List<EntityResolver> entityResolvers = new ArrayList<>();

    @Inject
    public AtlasEntityGraphDiscoveryV1(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Override
    public DiscoveredEntities discoverEntities(final List<AtlasEntity> entities) throws AtlasException {
        discoveredEntities.setRootEntities(entities);
        discover(entities);

        resolveReferences();

        return discoveredEntities;
    }

    private void resolveReferences() {
        for (EntityResolver resolver : entityResolvers ) {
            resolver.resolveEntityReferences(discoveredEntities);
        }

        if (discoveredEntities.hasDiscoveredEntities()) {

        }
    }


    private void discover(final List<AtlasEntity> entities) throws AtlasException {
        for (AtlasEntity entity : entities) {
            AtlasType type = typeRegistry.getType(entity.getTypeName());
            resolveEntity(type, entity, true);
        }
    }

    private void addToQueue(AtlasEntity entity) {
        if (!processedIds.contains(entity.getGuid())) {
            processedIds.add(entity.getGuid());

            discoveredEntities.addUnResolvedReference(entity);
        }
    }

    void resolveAttribute(AtlasType type, Object val) throws AtlasException {
        if (val != null) {
            if (type.getTypeCategory() == AtlasType.TypeCategory.ARRAY) {
                AtlasArrayType arrayType = (AtlasArrayType) type;
                AtlasType elemType = arrayType.getElementType();
                resolveCollectionReferences(elemType, val);
            } else if (type.getTypeCategory() == AtlasType.TypeCategory.MAP) {
                AtlasType keyType = ((AtlasMapType) type).getKeyType();
                AtlasType valueType = ((AtlasMapType) type).getValueType();
                resolveMapReferences(keyType, valueType, val);
            } else if (type.getTypeCategory() == AtlasType.TypeCategory.STRUCT) {
                resolveStructReferences(type, val);
            } else if (type.getTypeCategory() == AtlasType.TypeCategory.ENTITY) {
                addToQueue((AtlasEntity)val);
            }
        }
    }

    void resolveMapReferences(AtlasType keyType, AtlasType valueType, Object val) throws AtlasException {
        if (keyType.getTypeCategory() == AtlasType.TypeCategory.PRIMITIVE
            && valueType.getTypeCategory() == AtlasType.TypeCategory.PRIMITIVE) {
            return;
        }

        if (val != null) {
            Iterator<Map.Entry> it = null;
            if (Map.class.isAssignableFrom(val.getClass())) {
                it = ((Map) val).entrySet().iterator();
                ImmutableMap.Builder b = ImmutableMap.builder();
                while (it.hasNext()) {
                    Map.Entry e = it.next();
                    resolveAttribute(keyType, e.getKey());
                    resolveAttribute(valueType, e.getValue());
                }
            }
        }
    }

    void resolveCollectionReferences(AtlasType elemType, Object val) throws AtlasException {

        if (elemType.getTypeCategory() == AtlasType.TypeCategory.PRIMITIVE) {
            return;
        }

        if (val != null) {
            Iterator it = null;
            if (val instanceof Collection) {
                it = ((Collection) val).iterator();
            } else if (val instanceof Iterable) {
                it = ((Iterable) val).iterator();
            } else if (val instanceof Iterator) {
                it = (Iterator) val;
            }
            if (it != null) {
                while (it.hasNext()) {
                    Object elem = it.next();
                    resolveAttribute(elemType, elem);
                }
            }
        }
    }

    void resolveStructReferences(AtlasType type, Object val) throws AtlasException {

        if (val == null || !(val instanceof AtlasStruct)) {
            return;
        }

        AtlasStructType structType = (AtlasStructType) type;

        for (AtlasStructDef.AtlasAttributeDef sType : structType.getStructDefinition().getAttributeDefs()) {
            String attrType = sType.getTypeName();
            AtlasType typeDef = typeRegistry.getType(attrType);

            AtlasType.TypeCategory typeCategory = typeDef.getTypeCategory();
            if (typeCategory != AtlasType.TypeCategory.PRIMITIVE) {
                resolveAttribute(typeDef, val);
            }
        }
    }


    void resolveEntity(AtlasType type, AtlasEntity entity, boolean isRootEntity) throws AtlasException {
        if ( !isRootEntity) {
            discoveredEntities.addUnResolvedReference(entity);
        }

        resolveStructReferences(type, entity);
    }


}
