/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v1;


import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.AtlasEdgeLabel;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import static org.apache.atlas.repository.graph.GraphHelper.EDGE_LABEL_PREFIX;
import static org.apache.atlas.repository.graph.GraphHelper.string;

public abstract class DeleteHandlerV1 {

    public static final Logger LOG = LoggerFactory.getLogger(DeleteHandlerV1.class);

    private AtlasTypeRegistry typeRegistry;
    private boolean shouldUpdateReverseAttribute;
    private boolean softDelete;

    protected static final GraphHelper graphHelper = GraphHelper.getInstance();

    public DeleteHandlerV1(AtlasTypeRegistry typeRegistry, boolean shouldUpdateReverseAttribute, boolean softDelete) {
        this.typeRegistry = typeRegistry;
        this.shouldUpdateReverseAttribute = shouldUpdateReverseAttribute;
        this.softDelete = softDelete;
    }

    /**
     * Deletes the specified entity vertices.
     * Deletes any traits, composite entities, and structs owned by each entity.
     * Also deletes all the references from/to the entity.
     *
     * @param instanceVertices
     * @throws AtlasException
     */
    public void deleteEntities(List<AtlasVertex> instanceVertices) throws AtlasBaseException {
        RequestContextV1 requestContext = RequestContextV1.get();

        Set<AtlasVertex> deletionCandidateVertices = new HashSet<>();

        for (AtlasVertex instanceVertex : instanceVertices) {
            String guid = GraphHelper.getIdFromVertex(instanceVertex);
            AtlasEntity.Status state = AtlasGraphUtilsV1.getState(instanceVertex);
            if (requestContext.getDeletedEntityIds().contains(guid) || state == AtlasEntity.Status.STATUS_DELETED) {
                LOG.debug("Skipping deletion of {} as it is already deleted", guid);
                continue;
            }

            // Get GUIDs and vertices for all deletion candidates.
            Set<GraphHelper.VertexInfo> compositeVertices = getCompositeVertices(instanceVertex);

            // Record all deletion candidate GUIDs in RequestContext
            // and gather deletion candidate vertices.
            for (GraphHelper.VertexInfo vertexInfo : compositeVertices) {
                requestContext.recordEntityDelete(vertexInfo.getGuid());
                deletionCandidateVertices.add(vertexInfo.getVertex());
            }
        }

        // Delete traits and vertices.
        for (AtlasVertex deletionCandidateVertex : deletionCandidateVertices) {
            deleteAllTraits(deletionCandidateVertex);
            deleteTypeVertex(deletionCandidateVertex, false);
        }
    }

    /**
     * Get the GUIDs and vertices for all composite entities owned/contained by the specified root entity AtlasVertex.
     * The graph is traversed from the root entity through to the leaf nodes of the containment graph.
     *
     * @param entityVertex the root entity vertex
     * @return set of VertexInfo for all composite entities
     * @throws AtlasException
     */
    public Set<GraphHelper.VertexInfo> getCompositeVertices(AtlasVertex entityVertex) throws AtlasBaseException {
        Set<GraphHelper.VertexInfo> result = new HashSet<>();
        Stack<AtlasVertex> vertices = new Stack<>();
        vertices.push(entityVertex);
        while (vertices.size() > 0) {
            AtlasVertex vertex = vertices.pop();
            String typeName = GraphHelper.getTypeName(vertex);
            String guid = GraphHelper.getIdFromVertex(vertex);
            AtlasEntity.Status state = AtlasGraphUtilsV1.getState(vertex);
            if (state == AtlasEntity.Status.STATUS_DELETED) {
                //If the reference vertex is marked for deletion, skip it
                continue;
            }
            result.add(new GraphHelper.VertexInfo(guid, vertex, typeName));
            AtlasEntityType entityType = (AtlasEntityType) typeRegistry.getType(typeName);
            for (AtlasStructDef.AtlasAttributeDef attributeInfo : entityType.getAllAttributeDefs().values()) {
                if (!entityType.isMappedFromRefAttribute(attributeInfo.getName())) {
                    continue;
                }
                String edgeLabel = AtlasGraphUtilsV1.getAttributeEdgeLabel(entityType, attributeInfo.getName());
                AtlasType attrType = typeRegistry.getType(attributeInfo.getTypeName());
                switch (attrType.getTypeCategory()) {
                case ENTITY:
                    AtlasEdge edge = graphHelper.getEdgeForLabel(vertex, edgeLabel);
                    if (edge != null && AtlasGraphUtilsV1.getState(edge) == AtlasEntity.Status.STATUS_ACTIVE) {
                        AtlasVertex compositeVertex = edge.getInVertex();
                        vertices.push(compositeVertex);
                    }
                    break;
                case ARRAY:
                    AtlasArrayType arrType = (AtlasArrayType) attrType;
                    if (arrType.getElementType().getTypeCategory() != TypeCategory.ENTITY) {
                        continue;
                    }
                    Iterator<AtlasEdge> edges = graphHelper.getOutGoingEdgesByLabel(vertex, edgeLabel);
                    if (edges != null) {
                        while (edges.hasNext()) {
                            edge = edges.next();
                            if (edge != null && AtlasGraphUtilsV1.getState(edge) == AtlasEntity.Status.STATUS_ACTIVE) {
                                AtlasVertex compositeVertex = edge.getInVertex();
                                vertices.push(compositeVertex);
                            }
                        }
                    }
                    break;
                case MAP:
                    AtlasMapType mapType = (AtlasMapType) attrType;
                    TypeCategory valueTypeCategory = mapType.getValueType().getTypeCategory();
                    if (valueTypeCategory != TypeCategory.ENTITY) {
                        continue;
                    }
                    String propertyName = AtlasGraphUtilsV1.getQualifiedAttributePropertyKey(entityType, attributeInfo.getName());
                    List<String> keys = vertex.getProperty(propertyName, List.class);
                    if (keys != null) {
                        for (String key : keys) {
                            String mapEdgeLabel = GraphHelper.getQualifiedNameForMapKey(edgeLabel, key);
                            edge = graphHelper.getEdgeForLabel(vertex, mapEdgeLabel);
                            if (edge != null && AtlasGraphUtilsV1.getState(edge) == AtlasEntity.Status.STATUS_ACTIVE) {
                                AtlasVertex compositeVertex = edge.getInVertex();
                                vertices.push(compositeVertex);
                            }
                        }
                    }
                    break;
                default:
                }
            }
        }
        return result;
    }


    /**
     * Force delete is used to remove struct/trait in case of entity updates
     * @param edge
     * @param typeCategory
     * @param isComposite
     * @param forceDeleteStructTrait
     * @return returns true if the edge reference is hard deleted
     * @throws AtlasException
     */
    public boolean deleteEdgeReference(AtlasEdge edge, TypeCategory typeCategory, boolean isComposite,
        boolean forceDeleteStructTrait) throws AtlasBaseException {
        LOG.debug("Deleting {}", string(edge));
        boolean forceDelete =
            (isReference(typeCategory))
                ? forceDeleteStructTrait : false;
        if (isReference(typeCategory) && isComposite) {
            //If the vertex is of type struct/trait, delete the edge and then the reference vertex as the vertex is not shared by any other entities.
            //If the vertex is of type class, and its composite attribute, this reference vertex' lifecycle is controlled
            //through this delete, hence delete the edge and the reference vertex.
            AtlasVertex vertexForDelete = edge.getInVertex();

            //If deleting the edge and then the in vertex, reverse attribute shouldn't be updated
            deleteEdge(edge, false, forceDelete);
            deleteTypeVertex(vertexForDelete, typeCategory, forceDelete);
        } else {
            //If the vertex is of type class, and its not a composite attributes, the reference AtlasVertex' lifecycle is not controlled
            //through this delete. Hence just remove the reference edge. Leave the reference AtlasVertex as is

            //If deleting just the edge, reverse attribute should be updated for any references
            //For example, for the department type system, if the person's manager edge is deleted, subordinates of manager should be updated
            deleteEdge(edge, true, false);
        }
        return !softDelete || forceDelete;
    }

    protected void deleteEdge(AtlasEdge edge, boolean updateReverseAttribute, boolean force) throws AtlasBaseException {
        //update reverse attribute
        if (updateReverseAttribute) {
            AtlasEdgeLabel atlasEdgeLabel = new AtlasEdgeLabel(edge.getLabel());

            AtlasType parentType = typeRegistry.getType(atlasEdgeLabel.getTypeName());

            if (parentType instanceof AtlasStructType) {
                AtlasStructType parentStructType = (AtlasStructType) parentType;
                if (parentStructType.isForeignKeyAttribute(atlasEdgeLabel.getAttributeName()))
                    getAttributeForEdge(edge.getLabel());
                    deleteEdgeBetweenVertices(edge.getInVertex(), edge.getOutVertex(), atlasEdgeLabel.getAttributeName());
            }
        }

        deleteEdge(edge, force);

    }


    protected void deleteTypeVertex(AtlasVertex instanceVertex, TypeCategory typeCategory, boolean force) throws AtlasBaseException {
        switch (typeCategory) {
        case STRUCT:
        case CLASSIFICATION:
            deleteTypeVertex(instanceVertex, force);
            break;

        case ENTITY:
            deleteEntities(Collections.singletonList(instanceVertex));
            break;

        default:
            throw new IllegalStateException("Type category " + typeCategory + " not handled");
        }
    }

    /**
     * Deleting any type vertex. Goes over the complex attributes and removes the references
     * @param instanceVertex
     * @throws AtlasException
     */
    protected void deleteTypeVertex(AtlasVertex instanceVertex, boolean force) throws AtlasBaseException {
        LOG.debug("Deleting {}", string(instanceVertex));
        String typeName = GraphHelper.getTypeName(instanceVertex);


        AtlasType parentType = typeRegistry.getType(typeName);

        if (parentType instanceof AtlasStructType) {

            AtlasStructType entityType = (AtlasStructType) parentType;
            for (AtlasStructDef.AtlasAttributeDef attributeInfo : getAttributeDefs(entityType)) {
                LOG.debug("Deleting attribute {} for {}", attributeInfo.getName(), string(instanceVertex));

                AtlasType attrType = typeRegistry.getType(attributeInfo.getTypeName());

                String edgeLabel = AtlasGraphUtilsV1.getAttributeEdgeLabel(entityType, attributeInfo.getName());

                switch (attrType.getTypeCategory()) {
                case ENTITY:
                    //If its class attribute, delete the reference
                    deleteEdgeReference(instanceVertex, edgeLabel, TypeCategory.ENTITY, entityType.isMappedFromRefAttribute(attributeInfo.getName()));
                    break;

                case STRUCT:
                    //If its struct attribute, delete the reference
                    deleteEdgeReference(instanceVertex, edgeLabel, TypeCategory.STRUCT, false);
                    break;

                case ARRAY:
                    //For array attribute, if the element is struct/class, delete all the references
                    AtlasArrayType arrType = (AtlasArrayType) attrType;
                    AtlasType elemType = arrType.getElementType();
                    if (isReference(elemType.getTypeCategory())) {
                        Iterator<AtlasEdge> edges = graphHelper.getOutGoingEdgesByLabel(instanceVertex, edgeLabel);
                        if (edges != null) {
                            while (edges.hasNext()) {
                                AtlasEdge edge = edges.next();
                                deleteEdgeReference(edge, elemType.getTypeCategory(), entityType.isMappedFromRefAttribute(attributeInfo.getName()), false);
                            }
                        }
                    }
                    break;

                case MAP:
                    //For map attribute, if the value type is struct/class, delete all the references
                    AtlasMapType mapType = (AtlasMapType) attrType;
                    AtlasType keyType = mapType.getKeyType();
                    TypeCategory valueTypeCategory = mapType.getValueType().getTypeCategory();
                    String propertyName = AtlasGraphUtilsV1.getQualifiedAttributePropertyKey(entityType, attributeInfo.getName());

                    if (isReference(valueTypeCategory)) {
                        List<Object> keys = ArrayVertexMapper.getArrayElementsProperty(keyType, instanceVertex, propertyName);
                        if (keys != null) {
                            for (Object key : keys) {
                                String mapEdgeLabel = GraphHelper.getQualifiedNameForMapKey(edgeLabel, (String) key);
                                deleteEdgeReference(instanceVertex, mapEdgeLabel, valueTypeCategory, entityType.isMappedFromRefAttribute(attributeInfo.getName()));
                            }
                        }
                    }
                }
            }
        }

        deleteVertex(instanceVertex, force);
    }

    public void deleteEdgeReference(AtlasVertex outVertex, String edgeLabel, TypeCategory typeCategory,
        boolean isComposite) throws AtlasBaseException {
        AtlasEdge edge = graphHelper.getEdgeForLabel(outVertex, edgeLabel);
        if (edge != null) {
            deleteEdgeReference(edge, typeCategory, isComposite, false);
        }
    }

    /**
     * Delete all traits from the specified vertex.
     * @param instanceVertex
     * @throws AtlasException
     */
    private void deleteAllTraits(AtlasVertex instanceVertex) throws AtlasBaseException {
        List<String> traitNames = GraphHelper.getTraitNames(instanceVertex);
        LOG.debug("Deleting traits {} for {}", traitNames, string(instanceVertex));
        String typeName = GraphHelper.getTypeName(instanceVertex);

        for (String traitNameToBeDeleted : traitNames) {
            String relationshipLabel = GraphHelper.getTraitLabel(typeName, traitNameToBeDeleted);
            deleteEdgeReference(instanceVertex, relationshipLabel, TypeCategory.CLASSIFICATION, false);
        }
    }

    protected AtlasStructDef.AtlasAttributeDef getAttributeForEdge(String edgeLabel) throws AtlasBaseException {
        AtlasEdgeLabel atlasEdgeLabel = new AtlasEdgeLabel(edgeLabel);
//        IDataType referenceType = typeSystem.getDataType(IDataType.class, atlasEdgeLabel.getTypeName());

        AtlasType parentType = typeRegistry.getType(atlasEdgeLabel.getTypeName());
        AtlasStructType parentStructType = (AtlasStructType) parentType;

//        return getFieldMapping(referenceType).fields.get(atlasEdgeLabel.getAttributeName());
        return parentStructType.getAttributeDef(atlasEdgeLabel.getAttributeName());
    }

    protected abstract void _deleteVertex(AtlasVertex instanceVertex, boolean force);

    protected abstract void deleteEdge(AtlasEdge edge, boolean force) throws AtlasBaseException;

    /**
     * Deletes the edge between outvertex and inVertex. The edge is for attribute attributeName of outVertex
     * @param outVertex
     * @param inVertex
     * @param attributeName
     * @throws AtlasException
     */
    protected void deleteEdgeBetweenVertices(AtlasVertex outVertex, AtlasVertex inVertex, String attributeName) throws AtlasBaseException {
        LOG.debug("Removing edge from {} to {} with attribute name {}", string(outVertex), string(inVertex),
            attributeName);
        String typeName = GraphHelper.getTypeName(outVertex);
        String outId = GraphHelper.getIdFromVertex(outVertex);
        AtlasEntity.Status state = AtlasGraphUtilsV1.getState(outVertex);
        if ((outId != null && RequestContext.get().isDeletedEntity(outId)) || state == AtlasEntity.Status.STATUS_DELETED) {
            //If the reference vertex is marked for deletion, skip updating the reference
            return;
        }

        AtlasType parentType = typeRegistry.getType(typeName);
        String propertyName = AtlasGraphUtilsV1.getQualifiedAttributePropertyKey(parentType, attributeName);
        String edgeLabel = EDGE_LABEL_PREFIX + propertyName;
        AtlasEdge edge = null;

        AtlasStructType parentStructType = (AtlasStructType) parentType;
        AtlasStructDef.AtlasAttributeDef attrDef = parentStructType.getAttributeDef(attributeName);
        AtlasType attrType = typeRegistry.getType(attrDef.getTypeName());

        switch (attrType.getTypeCategory()) {
        case ENTITY:
            //If its class attribute, its the only edge between two vertices
            if (attrDef.getIsOptional()) {
                edge = graphHelper.getEdgeForLabel(outVertex, edgeLabel);
                if (shouldUpdateReverseAttribute) {
                    GraphHelper.setProperty(outVertex, propertyName, null);
                }
            } else {
                // Cannot unset a required attribute.
                throw new AtlasBaseException("Cannot unset required attribute " + propertyName +
                    " on " + GraphHelper.getVertexDetails(outVertex) + " edge = " + edgeLabel);
            }
            break;

        case ARRAY:
            //If its array attribute, find the right edge between the two vertices and update array property
            List<String> elements = GraphHelper.getListProperty(outVertex, propertyName);
            if (elements != null) {
                elements = new ArrayList<>(elements);   //Make a copy, else list.remove reflects on titan.getProperty()
                for (String elementEdgeId : elements) {
                    AtlasEdge elementEdge = graphHelper.getEdgeByEdgeId(outVertex, edgeLabel, elementEdgeId);
                    if (elementEdge == null) {
                        continue;
                    }

                    AtlasVertex elementVertex = elementEdge.getInVertex();
                    if (elementVertex.equals(inVertex)) {
                        edge = elementEdge;

                        //TODO element.size includes deleted items as well. should exclude
                        if (!attrDef.getIsOptional()
                            && elements.size() <= attrDef.getValuesMinCount()) {
                            // Deleting this edge would violate the attribute's lower bound.
                            throw new AtlasBaseException(
                                "Cannot remove array element from required attribute " +
                                    propertyName + " on "
                                    + GraphHelper.getVertexDetails(outVertex) + " " + GraphHelper.getEdgeDetails(elementEdge));
                        }

                        if (shouldUpdateReverseAttribute) {
                            //if composite attribute, remove the reference as well. else, just remove the edge
                            //for example, when table is deleted, process still references the table
                            //but when column is deleted, table will not reference the deleted column
                            LOG.debug("Removing edge {} from the array attribute {}", string(elementEdge),
                                attributeName);
                            elements.remove(elementEdge.getId().toString());
                            GraphHelper.setProperty(outVertex, propertyName, elements);
                            break;

                        }
                    }
                }
            }
            break;

        case MAP:
            //If its map attribute, find the right edge between two vertices and update map property
            List<String> keys = GraphHelper.getListProperty(outVertex, propertyName);
            if (keys != null) {
                keys = new ArrayList<>(keys);   //Make a copy, else list.remove reflects on titan.getProperty()
                for (String key : keys) {
                    String keyPropertyName = GraphHelper.getQualifiedNameForMapKey(propertyName, key);
                    String mapEdgeId = GraphHelper.getSingleValuedProperty(outVertex, keyPropertyName, String.class);
                    AtlasEdge mapEdge = graphHelper.getEdgeByEdgeId(outVertex, keyPropertyName, mapEdgeId);
                    if(mapEdge != null) {
                        AtlasVertex mapVertex = mapEdge.getInVertex();
                        if (mapVertex.getId().toString().equals(inVertex.getId().toString())) {
                            //TODO keys.size includes deleted items as well. should exclude
                            if (attrDef.getIsOptional() || keys.size() > attrDef.getValuesMinCount()) {
                                edge = mapEdge;
                            } else {
                                // Deleting this entry would violate the attribute's lower bound.
                                throw new AtlasBaseException(
                                    "Cannot remove map entry " + keyPropertyName + " from required attribute " +
                                        propertyName + " on " + GraphHelper.getVertexDetails(outVertex) + " " + GraphHelper.getEdgeDetails(mapEdge));
                            }

                            if (shouldUpdateReverseAttribute) {
                                //remove this key
                                LOG.debug("Removing edge {}, key {} from the map attribute {}", string(mapEdge), key,
                                    attributeName);
                                keys.remove(key);
                                GraphHelper.setProperty(outVertex, propertyName, keys);
                                GraphHelper.setProperty(outVertex, keyPropertyName, null);
                            }
                            break;
                        }
                    }
                }
            }
            break;

        case STRUCT:
        case CLASSIFICATION:
            break;

        default:
            throw new IllegalStateException("There can't be an edge from " + GraphHelper.getVertexDetails(outVertex) + " to "
                + GraphHelper.getVertexDetails(inVertex) + " with attribute name " + attributeName + " which is not class/array/map attribute");
        }

        if (edge != null) {
            deleteEdge(edge, false);
            RequestContext requestContext = RequestContext.get();
            GraphHelper.setProperty(outVertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY,
                requestContext.getRequestTime());
            GraphHelper.setProperty(outVertex, Constants.MODIFIED_BY_KEY, requestContext.getUser());
            requestContext.recordEntityUpdate(outId);
        }
    }

    protected void deleteVertex(AtlasVertex instanceVertex, boolean force) throws AtlasBaseException {
        //Update external references(incoming edges) to this vertex
        LOG.debug("Setting the external references to {} to null(removing edges)", string(instanceVertex));

        for (AtlasEdge edge : (Iterable<AtlasEdge>) instanceVertex.getEdges(AtlasEdgeDirection.IN)) {
            AtlasEntity.Status edgeState = AtlasGraphUtilsV1.getState(edge);
            if (edgeState == AtlasEntity.Status.STATUS_ACTIVE) {
                //Delete only the active edge references
                AtlasStructDef.AtlasAttributeDef attribute = getAttributeForEdge(edge.getLabel());
                //TODO use delete edge instead??
                deleteEdgeBetweenVertices(edge.getOutVertex(), edge.getInVertex(), attribute.getName());
            }
        }
        _deleteVertex(instanceVertex, force);
    }

    private boolean isReference(TypeCategory typeCategory) {
        return typeCategory == TypeCategory.STRUCT ||
            typeCategory == TypeCategory.ENTITY ||
            typeCategory == TypeCategory.CLASSIFICATION;
    }

    private Collection<AtlasStructDef.AtlasAttributeDef> getAttributeDefs(AtlasStructType structType) {
        Collection<AtlasStructDef.AtlasAttributeDef> ret = null;

        if (structType.getTypeCategory() == TypeCategory.STRUCT) {
            ret = structType.getStructDef().getAttributeDefs();
        } else if (structType.getTypeCategory() == TypeCategory.CLASSIFICATION) {
            ret = ((AtlasClassificationType)structType).getAllAttributeDefs().values();
        } else if (structType.getTypeCategory() == TypeCategory.ENTITY) {
            ret = ((AtlasEntityType)structType).getAllAttributeDefs().values();
        } else {
            ret = Collections.emptyList();
        }

        return ret;
    }

}
