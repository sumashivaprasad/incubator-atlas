package org.apache.atlas.repository.store.graph.v1;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.Singleton;
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

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.atlas.repository.graph.GraphHelper.string;

public class ArrayVertexMapper implements InstanceGraphMapper<List> {

    private static final Logger LOG = LoggerFactory.getLogger(ArrayVertexMapper.class);

    protected DeleteHandlerV1 deleteHandler;

    protected StructVertexMapper structVertexMapper;

    @Inject
    public ArrayVertexMapper(DeleteHandlerV1 deleteHandler) {
        this.deleteHandler = deleteHandler;
    }

    void init(StructVertexMapper structVertexMapper) {
        this.structVertexMapper = structVertexMapper;
    }

    public List toGraph(GraphMutationContext ctx) throws AtlasBaseException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping instance to vertex {} for array attribute {}", string(ctx.getReferringVertex()), ctx.getAttrType().getTypeName());
        }

        List newElements = (List) ctx.getValue();
        boolean newAttributeEmpty = (newElements == null || newElements.isEmpty());

        AtlasArrayType arrType = (AtlasArrayType) ctx.getAttrType();
        AtlasType elementType = arrType.getElementType();
        List<Object> currentElements = getArrayElementsProperty(elementType, ctx.getReferringVertex(), ctx.getVertexPropertyKey());

        List<Object> newElementsCreated = new ArrayList<>();

        if (!newAttributeEmpty) {
            if (newElements != null && !newElements.isEmpty()) {
                for (int index = 0; index < newElements.size(); index++) {
                    Optional<AtlasEdge> existingEdge = getEdgeIfExists(arrType, currentElements, newElements, index);
                    ctx.setCurrentEdge(existingEdge);

                    GraphMutationContext arrCtx = new GraphMutationContext.Builder(ctx.getParentType(), ctx.getStructDef(), ctx.getAttributeDef(),
                        arrType.getElementType(), newElements.get(index))
                        .referringVertex(ctx.getReferringVertex())
                        .edge(existingEdge)
                        .vertexProperty(ctx.getVertexPropertyKey()).build();

                    Object newEntry = structVertexMapper.mapCollectionElementsToVertex(arrCtx);
                    newElementsCreated.add(newEntry);
                }
            }
        }

        if (AtlasGraphUtilsV1.isReference(elementType)) {
            List<AtlasEdge> additionalEdges = removeUnusedArrayEntries(ctx.getParentType(), ctx.getAttributeDef(), (List) currentElements, (List) newElementsCreated, elementType);
            newElementsCreated.addAll(additionalEdges);
        }

        // for dereference on way out
        setArrayElementsProperty(elementType, ctx.getReferringVertex(), ctx.getVertexPropertyKey(), newElementsCreated);
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

    private Optional<AtlasEdge> getEdgeIfExists(AtlasArrayType arrType, List<Object> currentElements, List<Object> newElements, int index) {
        Optional<AtlasEdge> existingEdge = Optional.absent();
        if ( AtlasGraphUtilsV1.isReference(arrType.getElementType()) ) {
            Object currentElement = (currentElements != null && index < currentElements.size()) ?
                currentElements.get(index) : null;
            LOG.debug("Adding/updating element at position {}, current element {}, new element {}", index,
                currentElement, newElements.get(index));

            if ( currentElement != null) {
                existingEdge = Optional.of((AtlasEdge) currentElement);
            }
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
