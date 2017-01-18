package org.apache.atlas.repository.store.graph.v1;


import com.google.common.base.Optional;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;

import java.util.Objects;

public class GraphMutationContext {

    /**
     * Current entity/struct type for which we are mapping attributes
     */
    private AtlasStructType parentType;

    /**
     * Current attribute definition
     */
    private AtlasStructDef.AtlasAttributeDef attributeDef;

    /**
     * Current attribute type
     */
    private AtlasType attrType;

    /**
     * Current attribute value/entity/Struct instance
     */
    private Object value;

    /**
     *
     * The vertex which corresponds to the entity/struct for which we are mapping a complex attributes like struct, traits
     */
    AtlasVertex referringVertex;

    /**
     * the vertex property that we are updating
     */

    String vertexPropertyKey;

    /**
     * The current edge(in case of updates) from the parent entity/struct to the complex attribute like struct, trait
     */
    Optional<AtlasEdge> existingEdge;


    private GraphMutationContext(final Builder builder) {
        this.parentType = builder.parentType;
        this.attrType = builder.attrType;
        this.attributeDef = builder.attributeDef;
        this.existingEdge = builder.currentEdge;
        this.value = builder.currentValue;
        this.referringVertex = builder.referringVertex;
        this.vertexPropertyKey = builder.vertexPropertyKey;
    }

    public String getVertexPropertyKey() {
        return vertexPropertyKey;
    }

    @Override
    public int hashCode() {
        return Objects.hash(parentType, attrType, value, referringVertex, vertexPropertyKey, existingEdge);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        } else if (obj == this) {
            return true;
        } else if (obj.getClass() != getClass()) {
            return false;
        } else {
            GraphMutationContext rhs = (GraphMutationContext) obj;
            return Objects.equals(parentType, rhs.getParentType())
                 && Objects.equals(attrType, rhs.getAttrType())
                 && Objects.equals(value, rhs.getValue())
                 && Objects.equals(referringVertex, rhs.getReferringVertex())
                 && Objects.equals(vertexPropertyKey, rhs.getReferringVertex())
                 && Objects.equals(existingEdge, rhs.getCurrentEdge());
        }
    }


    public static final class Builder {

        private final AtlasStructType parentType;

        private final AtlasStructDef.AtlasAttributeDef attributeDef;

        private final AtlasType attrType;

        private final Object currentValue;

        private AtlasVertex referringVertex;

        private Optional<AtlasEdge> currentEdge = Optional.absent();

        private  String vertexPropertyKey;


        public Builder(AtlasStructType parentType, AtlasStructDef.AtlasAttributeDef attributeDef, AtlasType attrType, Object currentValue) {
            this.parentType = parentType;
            this.attributeDef = attributeDef;
            this.attrType = attrType;
            this.currentValue = currentValue;
        }

        Builder referringVertex(AtlasVertex referringVertex) {
            this.referringVertex = referringVertex;
            return this;
        }

        Builder edge(AtlasEdge edge) {
            this.currentEdge = Optional.of(edge);
            return this;
        }

        Builder edge(Optional<AtlasEdge> edge) {
            this.currentEdge = edge;
            return this;
        }

        Builder vertexProperty(String propertyKey) {
            this.vertexPropertyKey = propertyKey;
            return this;
        }

        GraphMutationContext build() {
            return new GraphMutationContext(this);
        }
    }

    public AtlasStructType getParentType() {
        return parentType;
    }

    public AtlasStructDef getStructDef() {
        return parentType.getStructDef();
    }

    public AtlasStructDef.AtlasAttributeDef getAttributeDef() {
        return attributeDef;
    }

    public AtlasType getAttrType() {
        return attrType;
    }

    public Object getValue() {
        return value;
    }

    public AtlasVertex getReferringVertex() {
        return referringVertex;
    }

    public Optional<AtlasEdge> getCurrentEdge() {
        return existingEdge;
    }

    public void setAttrType(final AtlasType attrType) {
        this.attrType = attrType;
    }
}
