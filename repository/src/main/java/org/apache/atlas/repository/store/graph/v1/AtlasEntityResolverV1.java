package org.apache.atlas.repository.store.graph.v1;

import com.google.common.collect.ImmutableMap;
import org.apache.atlas.AtlasException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.repository.store.graph.EntityReferenceResolver;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;


public class AtlasEntityReferenceResolver implements EntityReferenceResolver {

    Queue<AtlasEntity> queue = new LinkedList<AtlasEntity>();
    List<String> processedEntities = new ArrayList<>();

    @Override
    public Pair<List<AtlasEntity>, List<AtlasEntity>> resolveReferences(final AtlasEntity entity) {
        queue.add(entity);
        resolveEntities();
    }


    private void resolveEntities() {
        while (!queue.isEmpty()) {
            AtlasEntity r = queue.poll();
            if(r != null) {
               resolveEntity(r);
            }
        }
    }

    private void addToQueue(AtlasEntity entity) {
        if (!processedEntities.contains(entity.getGuid())) {
            processedEntities.add(entity.getGuid());
            if ( entity.isAssigned()) {
                queue.add(entity);
            }
        }
    }

    void resolveAttributes(AtlasType type, Object val) throws AtlasException {
        if (val != null) {
            if (type.getTypeCategory() == DataTypes.TypeCategory.ARRAY) {
                AtlasArrayType arrayType = (AtlasArrayType) dT;
                AtlasType elemType = arrayType.getElementType();
                resolveCollectionReferences(elemType, val);
            } else if (type.getTypeCategory() == DataTypes.TypeCategory.MAP) {
                AtlasMapType mapType = (AtlasMapType) type;
                AtlasType keyType = ((AtlasMapType) type).getKeyType();
                AtlasType valueType = ((AtlasMapType) type).getValueType();
                resolveMapReferences(keyType, valueType, val);
            } else if (type.getTypeCategory() == DataTypes.TypeCategory.STRUCT) {
                resolveStructReferences(type, val);
            } else if (type.getTypeCategory() == DataTypes.TypeCategory.CLASS) {
                resolveEntity(type, (AtlasEntity) val);
            }
        }
    }

    void resolveMapReferences(AtlasType keyType, AtlasType valueType, Object val) throws AtlasException {
        if (keyType.getTypeCategory() == DataTypes.TypeCategory.PRIMITIVE
            && valueType.getTypeCategory() == DataTypes.TypeCategory.PRIMITIVE) {
            return;
        }

        if (val != null) {
            Iterator<Map.Entry> it = null;
            if (Map.class.isAssignableFrom(val.getClass())) {
                it = ((Map) val).entrySet().iterator();
                ImmutableMap.Builder b = ImmutableMap.builder();
                while (it.hasNext()) {
                    Map.Entry e = it.next();
                    resolveAttributes(keyType, e.getKey());
                    resolveAttributes(valueType, e.getValue());
                }
            }
        }
    }

    void resolveCollectionReferences(AtlasType elemType, Object val) throws AtlasException {

        if (elemType.getTypeCategory() == DataTypes.TypeCategory.PRIMITIVE) {
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
                DataTypes.TypeCategory elemCategory = elemType.getTypeCategory();
                while (it.hasNext()) {
                    Object elem = it.next();
                    resolveAttributes(elemType, elem);
                }
            }
        }
    }

    void resolveStructReferences(AtlasType type, Object val) throws AtlasException {

        if (val == null || !(val instanceof AtlasStruct)) {
            return;
        }

        AtlasStructType structType = (AtlasStructType) type;
        AtlasStruct i = (AtlasStruct) val;

        for (Map.Entry<String, AttributeInfo> sType : structType.) {
            AttributeInfo aInfo = e.getValue();
            String attrName = e.getKey();
            if (aInfo.dataType().getTypeCategory() != DataTypes.TypeCategory.PRIMITIVE) {
                Object aVal = i.get(attrName);
                nodeProcessor.processNode(new Node(i, attrName, aInfo, aVal));
                resolveAttributes(aInfo.dataType(), aVal);
            }
        }
    }

    void resolveEntity(AtlasEntity entity) {
        if (entity == null || !(entity.isAssigned())) {
            return;
        }
        addToQueue(entity);
    }
}
