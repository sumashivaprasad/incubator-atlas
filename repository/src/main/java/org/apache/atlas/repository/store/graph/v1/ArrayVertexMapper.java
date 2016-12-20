package org.apache.atlas.repository.store.graph.v1;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.apache.atlas.aspect.Monitored;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.atlas.repository.graph.GraphHelper.string;

public class ArrayVertexMapper {

    private static final Logger LOG = LoggerFactory.getLogger(ArrayVertexMapper.class);

    private DeleteHandlerV1 deleteHandler;

    private StructVertexMapper structVertexMapper = new StructVertexMapper();

    @Inject
    public ArrayVertexMapper(DeleteHandlerV1 deleteHandler) {
        this.deleteHandler = deleteHandler;
    }

    public Collection<Object> toVertex(AtlasStructType parentType, AtlasStructDef.AtlasAttributeDef attributeDef, AtlasArrayType arrType, Object val, AtlasVertex instanceVertex, String vertexPropertyName) throws AtlasBaseException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping instance to vertex {} for array attribute {}", string(instanceVertex), arrType.getTypeName());
        }

        List newElements = (List) val;
        boolean newAttributeEmpty = (newElements == null || newElements.isEmpty());

        AtlasType elementType = arrType.getElementType();
        List<Object> currentElements = getArrayElementsProperty(elementType, instanceVertex, vertexPropertyName);

        List<Object> newElementsCreated = new ArrayList<>();

        if (!newAttributeEmpty) {
            if (newElements != null && !newElements.isEmpty()) {
                int index = 0;
                for (; index < newElements.size(); index++) {
                    Object currentElement = (currentElements != null && index < currentElements.size()) ?
                        currentElements.get(index) : null;
                    LOG.debug("Adding/updating element at position {}, current element {}, new element {}", index,
                        currentElement, newElements.get(index));
                    Optional<AtlasEdge> existingEdge = getEdgeIfExists(arrType, currentElements, index);
                    Object newEntry = structVertexMapper.mapCollectionElementsToVertex(parentType, attributeDef, elementType, val, instanceVertex, vertexPropertyName, existingEdge);
                    newElementsCreated.add(newEntry);
                }
            }
        }

        if (AtlasGraphUtilsV1.isReference(elementType)) {
            List<AtlasEdge> additionalEdges = removeUnusedArrayEntries(parentType, attributeDef, (List) currentElements, (List) newElementsCreated, elementType);
            newElementsCreated.addAll(additionalEdges);
        }

        // for dereference on way out
        setArrayElementsProperty(elementType, instanceVertex, vertexPropertyName, newElementsCreated);
        return newElementsCreated;
    }

    //Removes unused edges from the old collection, compared to the new collection
    private List<AtlasEdge> removeUnusedArrayEntries(
        AtlasStructType entityType,
        AtlasStructDef.AtlasAttributeDef attributeDef,
        List<AtlasEdge> currentEntries,
        List<AtlasEdge> newEntries,
        AtlasType entryType) throws AtlasBaseException {
        if (currentEntries != null && !currentEntries.isEmpty()) {
            LOG.debug("Removing unused entries from the old collection");
            if (AtlasGraphUtilsV1.isReference(entryType)) {

                //Remove the edges for (current edges - new edges)
                List<AtlasEdge> cloneElements = new ArrayList<>(currentEntries);
                cloneElements.removeAll(newEntries);
                List<AtlasEdge> additionalElements = new ArrayList<>();
                LOG.debug("Removing unused entries from the old collection - {}", cloneElements);

                if (!cloneElements.isEmpty()) {
                    for (AtlasEdge edge : cloneElements) {
                        boolean deleteChildReferences = StructVertexMapper.shouldManageChildReferences(entityType, attributeDef.getName());
                        boolean deleted = deleteHandler.deleteEdgeReference(edge, entryType.getTypeCategory(),
                            deleteChildReferences, true);
                        if (!deleted) {
                            additionalElements.add(edge);
                        }
                    }
                }
                return additionalElements;
            }
        }
        return new ArrayList<>();
    }

    @Monitored
    public static List<Object> getArrayElementsProperty(AtlasType elementType, AtlasVertex instanceVertex, String propertyName) {
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        if (AtlasGraphUtilsV1.isReference(elementType)) {
            return (List)instanceVertex.getListProperty(actualPropertyName, AtlasEdge.class);
        }
        else {
            return (List)instanceVertex.getListProperty(actualPropertyName);
        }
    }

    private Optional<AtlasEdge> getEdgeIfExists(AtlasArrayType arrType, List<Object> currentList, int index) {
        Optional<AtlasEdge> existingEdge = Optional.absent();
        if ( AtlasGraphUtilsV1.isReference(arrType.getElementType()) ) {
            existingEdge = Optional.of((AtlasEdge) currentList.get(index));
        }

        return existingEdge;
    }

    @Monitored
    private void setArrayElementsProperty(AtlasType elementType, AtlasVertex instanceVertex, String propertyName, List<Object> values) {
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        if (AtlasGraphUtilsV1.isReference(elementType)) {
            GraphHelper.setListPropertyFromElementIds(instanceVertex, actualPropertyName, (List) values);
        }
        else {
            GraphHelper.setProperty(instanceVertex, actualPropertyName, values);
        }
    }
}
