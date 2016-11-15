package org.apache.atlas.repository.store.graph.v1;

import com.google.common.collect.ImmutableMap;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.store.graph.DiscoveredEntities;
import org.apache.atlas.repository.store.graph.EntityGraphDiscovery;
import org.apache.atlas.repository.store.graph.EntityResolver;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasEntityType;
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
    public DiscoveredEntities discoverEntities(final List<AtlasEntity> entities) throws AtlasBaseException {

        discover(entities);

        resolveReferences();
        return discoveredEntities;
    }

    private void resolveReferences() throws AtlasBaseException {
        for (EntityResolver resolver : entityResolvers ) {
            resolver.resolveEntityReferences(discoveredEntities);
        }

        if (discoveredEntities.hasUnresolvedReferences()) {

        }
    }


    private void discover(final List<AtlasEntity> entities) throws AtlasBaseException {
        discoveredEntities.setRootEntities(entities);
        for (AtlasEntity entity : entities) {
            AtlasType type = typeRegistry.getType(entity.getTypeName());
            walkEntityGraph(type, entity);
        }
    }

    private void visitReference(AtlasEntityType type, Object entity, boolean isComposite) {

        if ( entity != null) {
            if ( entity instanceof String ) {
                String entityId = (String) entity;
                discoveredEntities.addUnResolvedIdReference(type, entityId);

            } else if ( entity instanceof  AtlasEntity ) {

                AtlasEntity entityObj = ( AtlasEntity ) entity;
                if (!processedIds.contains(entityObj.getGuid())) {
                    processedIds.add(entityObj.getGuid());

                    if ( isComposite ) {
                        discoveredEntities.addRootEntity(entityObj);
                    } else {
                        discoveredEntities.addUnResolvedEntityReference(entityObj);
                    }
                }
            }
        }


    }

    void visitAttribute(AtlasType type, Object val) throws AtlasBaseException {
        if (val != null) {
            if ( isPrimitive(type.getTypeCategory()) ) {
                return;
            }
            if (type.getTypeCategory() == TypeCategory.ARRAY) {
                AtlasArrayType arrayType = (AtlasArrayType) type;
                AtlasType elemType = arrayType.getElementType();
                visitCollectionReferences(elemType, val);
            } else if (type.getTypeCategory() == TypeCategory.MAP) {
                AtlasType keyType = ((AtlasMapType) type).getKeyType();
                AtlasType valueType = ((AtlasMapType) type).getValueType();
                visitMapReferences(keyType, valueType, val);
            } else if (type.getTypeCategory() == TypeCategory.STRUCT) {
                visitStruct(type, val);
            }
        }
    }

    void visitMapReferences(AtlasType keyType, AtlasType valueType, Object val) throws AtlasBaseException {
        if (isPrimitive(keyType.getTypeCategory()) && isPrimitive(valueType.getTypeCategory())) {
            return;
        }

        if (val != null) {
            Iterator<Map.Entry> it = null;
            if (Map.class.isAssignableFrom(val.getClass())) {
                it = ((Map) val).entrySet().iterator();
                ImmutableMap.Builder b = ImmutableMap.builder();
                while (it.hasNext()) {
                    Map.Entry e = it.next();
                    visitAttribute(keyType, e.getKey());
                    visitAttribute(valueType, e.getValue());
                }
            }
        }
    }

    void visitCollectionReferences(AtlasType elemType, Object val) throws AtlasBaseException {

        if (isPrimitive(elemType.getTypeCategory())) {
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
                    visitAttribute(elemType, elem);
                }
            }
        }
    }

    void visitStruct(AtlasType type, Object val) throws AtlasBaseException {

        if (val == null || !(val instanceof AtlasStruct)) {
            return;
        }

        AtlasStructType structType = (AtlasStructType) type;

        for (AtlasStructDef.AtlasAttributeDef attributeDef : structType.getStructDef().getAttributeDefs()) {
            String attrName = attributeDef.getName();
            AtlasType attrType = structType.getAttributeType(attrName);

            TypeCategory typeCategory = attrType.getTypeCategory();

            //Handle isMappedFromRef(isComposite case)
            if (typeCategory == TypeCategory.ENTITY) {
                for (AtlasStructDef.AtlasConstraintDef constraintDef : attributeDef.getConstraintDefs()) {
                    //Attribute mapped from references - isComposite
                    if (AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_MAPPED_FROM_REF.equals(constraintDef.getType())) {
                        visitReference((AtlasEntityType) type,  val, true);
                    }
                }
            } else {
                visitAttribute(type, val);
            }
        }
    }


    void walkEntityGraph(AtlasType type, AtlasEntity entity) throws AtlasBaseException {
        visitStruct(type, entity);
    }


    boolean isPrimitive(TypeCategory typeCategory) {
        return typeCategory == TypeCategory.PRIMITIVE || typeCategory == TypeCategory.ENUM;
    }
}
