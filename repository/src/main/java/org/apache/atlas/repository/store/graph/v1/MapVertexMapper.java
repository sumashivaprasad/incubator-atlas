package org.apache.atlas.repository.store.graph.v1;


import com.google.inject.Inject;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.graph.GraphHelper.string;

public class MapVertexMapper {

    private DeleteHandlerV1 deleteHandler;

    private StructVertexMapper structVertexMapper = new StructVertexMapper();

    private static final Logger LOG = LoggerFactory.getLogger(MapVertexMapper.class);

    @Inject
    public MapVertexMapper(DeleteHandlerV1 deleteHandler) {
        this.deleteHandler = deleteHandler;
    }

    Map<String, Object> mapToVertex(AtlasStructType parentType, AtlasStructDef.AtlasAttributeDef attributeDef, AtlasMapType mapType, Object val, AtlasVertex vertex) throws AtlasBaseException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping instance to vertex {} for map type {}", mapType.getTypeName(), string(vertex));
        }

        @SuppressWarnings("unchecked") Map<Object, Object> newVal =
            (Map<Object, Object>) val;

        boolean newAttributeEmpty = MapUtils.isEmpty(newVal);

        String propertyName = AtlasGraphUtilsV1.getQualifiedAttributePropertyKey(parentType, attributeDef.getName());

        Map<String, Object> currentMap = new HashMap<>();
        Map<String, Object> newMap = new HashMap<>();

        try {

            List<String> currentKeys = GraphHelper.getListProperty(vertex, propertyName);
            if (currentKeys != null && !currentKeys.isEmpty()) {
                for (String key : currentKeys) {
                    String propertyNameForKey = GraphHelper.getQualifiedNameForMapKey(propertyName, key);
                    Object propertyValueForKey = getMapValueProperty(mapType.getValueType(), vertex, propertyNameForKey);
                    currentMap.put(key, propertyValueForKey);
                }
            }

            if (!newAttributeEmpty) {
                for (Map.Entry<Object, Object> entry : newVal.entrySet()) {
                    String keyStr = entry.getKey().toString();
                    String propertyNameForKey = GraphHelper.getQualifiedNameForMapKey(propertyName, keyStr);

                    Object newEntry = structVertexMapper.mapToVertexByTypeCategory(parentType, attributeDef, mapType.getValueType(), entry.getValue(), vertex);

                    //Add/Update/Remove property value
                    setMapValueProperty(mapType.getValueType(), vertex, propertyNameForKey, newEntry);
                    newMap.put(keyStr, newEntry);
                }
            }

            Map<String, Object> additionalMap =
                removeUnusedMapEntries(vertex, propertyName, currentMap, newMap, mapType, attributeDef);

            Set<String> newKeys = new HashSet<>(newMap.keySet());
            newKeys.addAll(additionalMap.keySet());

            // for dereference on way out

            GraphHelper.setListProperty(vertex, propertyName, new ArrayList<>(newKeys));
        } catch (AtlasException e) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }

        return newMap;
    }


    public static Object getMapValueProperty(AtlasType elementType, AtlasVertex instanceVertex, String propertyName) {
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        if (AtlasGraphUtilsV1.isReference(elementType)) {
            return instanceVertex.getProperty(actualPropertyName, AtlasEdge.class);
        }
        else {
            return instanceVertex.getProperty(actualPropertyName, String.class).toString();
        }
    }

    public static void setMapValueProperty(AtlasType elementType, AtlasVertex instanceVertex, String propertyName, Object value) {
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        if (AtlasGraphUtilsV1.isReference(elementType)) {
            instanceVertex.setPropertyFromElementId(actualPropertyName, (AtlasEdge)value);
        }
        else {
            instanceVertex.setProperty(actualPropertyName, value);
        }
    }

    //Remove unused entries from map
    private Map<String, Object> removeUnusedMapEntries(
        AtlasVertex instanceVertex, String propertyName,
        Map<String, Object> currentMap,
        Map<String, Object> newMap, AtlasMapType mapType, AtlasStructDef.AtlasAttributeDef attributeDef)
        throws AtlasException {

        Map<String, Object> additionalMap = new HashMap<>();
        for (String currentKey : currentMap.keySet()) {

            boolean shouldDeleteKey = !newMap.containsKey(currentKey);
            if (AtlasGraphUtilsV1.isReference(mapType.getValueType())) {

                //Delete the edge reference if its not part of new edges created/updated
                AtlasEdge currentEdge = (AtlasEdge)currentMap.get(currentKey);

                if (!newMap.values().contains(currentEdge)) {
                    boolean deleteChildReferences = StructVertexMapper.shouldManageChildReferences(attributeDef);
                    boolean deleted =
                        deleteHandler.deleteEdgeReference(currentEdge, mapType.getValueType().getTypeCategory(), deleteChildReferences, true);
                    if (!deleted) {
                        additionalMap.put(currentKey, currentEdge);
                        shouldDeleteKey = false;
                    }
                }
            }

            if (shouldDeleteKey) {
                String propertyNameForKey = GraphHelper.getQualifiedNameForMapKey(propertyName, currentKey);
                GraphHelper.setProperty(instanceVertex, propertyNameForKey, null);
            }
        }
        return additionalMap;
    }

}
