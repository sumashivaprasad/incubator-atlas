package org.apache.atlas.repository.store.graph.v1;

import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.IDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.atlas.repository.graph.GraphHelper.string;

public class ArrayVertexMapper {

    private static final Logger LOG = LoggerFactory.getLogger(ArrayVertexMapper.class);

    public Collection<Object> toVertex(AtlasStructType parentType, AtlasStructDef.AtlasAttributeDef attributeDef, AtlasArrayType arrType, Object val, AtlasVertex vertex, String vertexPropertyName) throws AtlasBaseException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping instance to vertex {} for array attribute {}", string(vertex), arrType.getTypeName());
        }

        List newElements = (List) val;
        boolean newAttributeEmpty = (newElements == null || newElements.isEmpty());

        IDataType elementType = ((DataTypes.ArrayType) attributeInfo.dataType()).getElemType();
        String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);

        List<Object> currentElements = GraphHelper.getArrayElementsProperty(elementType, instanceVertex, propertyName);

        List<Object> newElementsCreated = new ArrayList<>();

        if (!newAttributeEmpty) {
            if (newElements != null && !newElements.isEmpty()) {
                int index = 0;
                for (; index < newElements.size(); index++) {
                    Object currentElement = (currentElements != null && index < currentElements.size()) ?
                        currentElements.get(index) : null;
                    LOG.debug("Adding/updating element at position {}, current element {}, new element {}", index,
                        currentElement, newElements.get(index));
                    Object newEntry = addOrUpdateCollectionEntry(instanceVertex, attributeInfo, elementType,
                        newElements.get(index), currentElement, propertyName, operation);
                    newElementsCreated.add(newEntry);
                }
            }
        }

        if (GraphHelper.isReference(elementType)) {

            List<AtlasEdge> additionalEdges = removeUnusedEntries(instanceVertex, propertyName, (List) currentElements,
                (List) newElementsCreated, elementType, attributeInfo);
            newElementsCreated.addAll(additionalEdges);
        }

        // for dereference on way out
        GraphHelper.setArrayElementsProperty(elementType, instanceVertex, propertyName, newElementsCreated);


    }

    //Removes unused edges from the old collection, compared to the new collection
    private List<AtlasEdge> removeUnusedEntries(AtlasVertex instanceVertex, String edgeLabel,
        Collection<AtlasEdge> currentEntries,
        Collection<AtlasEdge> newEntries,
        IDataType entryType, AttributeInfo attributeInfo) throws AtlasException {
        if (currentEntries != null && !currentEntries.isEmpty()) {
            LOG.debug("Removing unused entries from the old collection");
            if (entryType.getTypeCategory() == DataTypes.TypeCategory.STRUCT
                || entryType.getTypeCategory() == DataTypes.TypeCategory.CLASS) {

                //Remove the edges for (current edges - new edges)
                List<AtlasEdge> cloneElements = new ArrayList<>(currentEntries);
                cloneElements.removeAll(newEntries);
                List<AtlasEdge> additionalElements = new ArrayList<>();
                LOG.debug("Removing unused entries from the old collection - {}", cloneElements);

                if (!cloneElements.isEmpty()) {
                    for (AtlasEdge edge : cloneElements) {
                        boolean deleted = deleteHandler.deleteEdgeReference(edge, entryType.getTypeCategory(),
                            attributeInfo.isComposite, true);
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
}
