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
     * Current entity/struct definition
     */
    private AtlasStructDef structDef;

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
    Optional<AtlasEdge> currentEdge;


    private GraphMutationContext(final Builder builder) {
        this.parentType = builder.parentType;
        this.attrType = builder.attrType;
        this.structDef = builder.structDef;
        this.attributeDef = builder.attributeDef;
        this.currentEdge = builder.currentEdge;
        this.value = builder.currentValue;
        this.referringVertex = builder.referringVertex;
        this.vertexPropertyKey = builder.vertexPropertyKey;
    }

    public String getVertexPropertyKey() {
        return vertexPropertyKey;
    }

    @Override
    public int hashCode() {
        int result = parentType != null ? parentType.hashCode() : 0;
        result = 31 * result + (structDef != null ? structDef.hashCode() : 0);
        result = 31 * result + (attributeDef != null ? attributeDef.hashCode() : 0);
        result = 31 * result + (attrType != null ? attrType.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (referringVertex != null ? referringVertex.hashCode() : 0);
        result = 31 * result + (currentEdge != null ? currentEdge.hashCode() : 0);
        result = 31 * result + (vertexPropertyKey != null ? vertexPropertyKey.hashCode() : 0);
        return result;
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
                 && Objects.equals(structDef, rhs.getStructDef())
                 && Objects.equals(attrType, rhs.getAttrType())
                 && Objects.equals(value, rhs.getValue())
                 && Objects.equals(referringVertex, rhs.getReferringVertex())
                 && Objects.equals(vertexPropertyKey, rhs.getReferringVertex())
                 && Objects.equals(currentEdge, rhs.getCurrentEdge());
        }
    }


    public static final class Builder {

        private final AtlasStructType parentType;

        private final AtlasStructDef structDef;

        private final AtlasStructDef.AtlasAttributeDef attributeDef;

        private final AtlasType attrType;

        private final Object currentValue;

        private AtlasVertex referringVertex;

        private Optional<AtlasEdge> currentEdge = Optional.absent();

        private  String vertexPropertyKey;


        public Builder(AtlasStructType parentType, AtlasStructDef structDef, AtlasStructDef.AtlasAttributeDef attributeDef, AtlasType attrType, Object currentValue) {
            this.parentType = parentType;
            this.structDef = structDef;
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
        return structDef;
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
        return currentEdge;
    }

    public void setReferringVertex(final AtlasVertex referringVertex) {
        this.referringVertex = referringVertex;
    }

    public void setCurrentEdge(final Optional<AtlasEdge> currentEdge) {
        this.currentEdge = currentEdge;
    }

    public void setVertexPropertyKey(final String vertexPropertyKey) {
        this.vertexPropertyKey = vertexPropertyKey;
    }
}
