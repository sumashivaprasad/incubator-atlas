package org.apache.atlas.repository.store.graph.v1;


import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.FieldMapping;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.atlas.repository.graph.GraphHelper.string;

public class DeleteHandlerV1 {

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
    public void deleteEntities(List<AtlasVertex> instanceVertices) throws AtlasException {
        RequestContext requestContext = RequestContext.get();

        Set<AtlasVertex> deletionCandidateVertices = new HashSet<>();

        for (AtlasVertex instanceVertex : instanceVertices) {
            String guid = GraphHelper.getIdFromVertex(instanceVertex);
            Id.EntityState state = GraphHelper.getState(instanceVertex);
            if (requestContext.getDeletedEntityIds().contains(guid) || state == Id.EntityState.DELETED) {
                LOG.debug("Skipping deletion of {} as it is already deleted", guid);
                continue;
            }

            // Get GUIDs and vertices for all deletion candidates.
            Set<GraphHelper.VertexInfo> compositeVertices = graphHelper.getCompositeVertices(instanceVertex);

            // Record all deletion candidate GUIDs in RequestContext
            // and gather deletion candidate vertices.
            for (GraphHelper.VertexInfo vertexInfo : compositeVertices) {
                requestContext.recordEntityDelete(vertexInfo.getGuid(), vertexInfo.getTypeName());
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
            (typeCategory == TypeCategory.STRUCT || typeCategory == TypeCategory.CLASSIFICATION)
                ? forceDeleteStructTrait : false;
        if (typeCategory == TypeCategory.STRUCT || typeCategory == TypeCategory.CLASSIFICATION
            || (typeCategory == TypeCategory.ENTITY && isComposite)) {
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
            AtlasStructDef.AtlasAttributeDef attributeInfo = getAttributeForEdge(edge.getLabel());
            if (attributeInfo.reverseAttributeName != null) {
                deleteEdgeBetweenVertices(edge.getInVertex(), edge.getOutVertex(),
                    attributeInfo.reverseAttributeName);
            }
        }

        deleteEdge(edge, force);
    }


    protected void deleteTypeVertex(AtlasVertex instanceVertex, DataTypes.TypeCategory typeCategory, boolean force) throws AtlasBaseException {
        switch (typeCategory) {
        case STRUCT:
        case TRAIT:
            deleteTypeVertex(instanceVertex, force);
            break;

        case CLASS:
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

        AtlasEntityType entityType = (AtlasEntityType) typeRegistry.getType(typeName);

        for (AtlasStructDef.AtlasAttributeDef attributeInfo : entityType.getAllAttributeDefs()) {
            LOG.debug("Deleting attribute {} for {}", attributeInfo.getName(), string(instanceVertex));

            AtlasType attrType = typeRegistry.getType(attributeInfo.getTypeName());

            String edgeLabel = AtlasGraphUtilsV1.getAttributeEdgeLabel(entityType, attributeInfo.getName());

            switch (attrType.getTypeCategory()) {
            case ENTITY:
                //If its class attribute, delete the reference
                deleteEdgeReference(instanceVertex, edgeLabel, DataTypes.TypeCategory.CLASS, ((AtlasEntityType) attrType).isMappedFromRefAttribute(attributeInfo.getName()));
                break;

            case STRUCT:
                //If its struct attribute, delete the reference
                deleteEdgeReference(instanceVertex, edgeLabel, DataTypes.TypeCategory.STRUCT, false);
                break;

            case ARRAY:
                //For array attribute, if the element is struct/class, delete all the references
                IDataType elementType = ((DataTypes.ArrayType) attributeInfo.dataType()).getElemType();
                DataTypes.TypeCategory elementTypeCategory = elementType.getTypeCategory();
                if (elementTypeCategory == DataTypes.TypeCategory.STRUCT ||
                    elementTypeCategory == DataTypes.TypeCategory.CLASS) {
                    Iterator<AtlasEdge> edges = graphHelper.getOutGoingEdgesByLabel(instanceVertex, edgeLabel);
                    if (edges != null) {
                        while (edges.hasNext()) {
                            AtlasEdge edge = edges.next();
                            deleteEdgeReference(edge, elementType.getTypeCategory(), attributeInfo.isComposite, false);
                        }
                    }
                }
                break;

            case MAP:
                //For map attribute, if the value type is struct/class, delete all the references
                DataTypes.MapType mapType = (DataTypes.MapType) attributeInfo.dataType();
                DataTypes.TypeCategory valueTypeCategory = mapType.getValueType().getTypeCategory();
                String propertyName = GraphHelper.getQualifiedFieldName(type, attributeInfo.name);

                if (valueTypeCategory == DataTypes.TypeCategory.STRUCT ||
                    valueTypeCategory == DataTypes.TypeCategory.CLASS) {
                    List<String> keys = GraphHelper.getListProperty(instanceVertex, propertyName);
                    if (keys != null) {
                        for (String key : keys) {
                            String mapEdgeLabel = GraphHelper.getQualifiedNameForMapKey(edgeLabel, key);
                            deleteEdgeReference(instanceVertex, mapEdgeLabel, valueTypeCategory, attributeInfo.isComposite);
                        }
                    }
                }
            }
        }

        deleteVertex(instanceVertex, force);
    }

    public void deleteEdgeReference(AtlasVertex outVertex, String edgeLabel, DataTypes.TypeCategory typeCategory,
        boolean isComposite) throws AtlasException {
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
    private void deleteAllTraits(AtlasVertex instanceVertex) throws AtlasException {
        List<String> traitNames = GraphHelper.getTraitNames(instanceVertex);
        LOG.debug("Deleting traits {} for {}", traitNames, string(instanceVertex));
        String typeName = GraphHelper.getTypeName(instanceVertex);

        for (String traitNameToBeDeleted : traitNames) {
            String relationshipLabel = GraphHelper.getTraitLabel(typeName, traitNameToBeDeleted);
            deleteEdgeReference(instanceVertex, relationshipLabel, DataTypes.TypeCategory.TRAIT, false);
        }
    }
}
