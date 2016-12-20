package org.apache.atlas.repository.store.graph.v1;


import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.repository.graph.GraphHelper.string;

public class DeleteHandlerV1 {

    public static final Logger LOG = LoggerFactory.getLogger(DeleteHandlerV1.class);

    private AtlasTypeRegistry typeRegistry;

    public DeleteHandlerV1(AtlasTypeRegistry typeRegistry, boolean shouldUpdateReverseAttribute, boolean softDelete) {
        this.typeRegistry = typeRegistry;
        this.shouldUpdateReverseAttribute = shouldUpdateReverseAttribute;
        this.softDelete = softDelete;
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
}
