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

package org.apache.atlas.repository.graph;

import com.google.common.base.Joiner;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.typesystem.IInstance;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.PrimaryKeyConstraint;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.atlas.repository.graph.GraphHelper.getQualifiedFieldName;

public class PrimaryKeyDedupHandler implements DedupHandler<ClassType, IReferenceableInstance> {

    private static final Logger LOG = LoggerFactory.getLogger(PrimaryKeyDedupHandler.class);
    private static final GraphHelper graphHelper = GraphHelper.getInstance();

    protected IReferenceableInstance instance;
    protected TypedInstanceToGraphMapper mapper;
    protected TypeSystem typeSystem;

    public PrimaryKeyDedupHandler(final TypedInstanceToGraphMapper typedInstanceToGraphMapper) {
        this.mapper = typedInstanceToGraphMapper;
        this.typeSystem = TypeSystem.getInstance();
    }

    @Override
    public boolean exists(final ClassType type, final IReferenceableInstance instance) throws AtlasException {
       return vertex(type, instance) != null;
    }

    @Override
    public Vertex vertex(final ClassType type, final IReferenceableInstance instance) throws AtlasException {
        Vertex instanceVertex = null;
        ClassType classType = typeSystem.getDataType(ClassType.class, instance.getTypeName());
        if (classType.hasPrimaryKey()) {
            LOG.debug("Checking if there is an instance with the same primary key for instance {}", instance.toShortString());
            PrimaryKeyConstraint primaryKey = classType.getPrimaryKey();
            instanceVertex = getVertexByProperties(classType, primaryKey.columns(), instance);
            if ( instanceVertex != null ) {
                LOG.debug("Found vertex by primary key {} ", Joiner.on(":").join(primaryKey.columns()));
            }
        }
        return instanceVertex;
    }

    private Vertex getVertexByProperties(final ClassType classType, final List<String> propertyKeys, final IReferenceableInstance ref) throws AtlasException {

        PrimaryKeyQueryContext ctx = addSearchClauses(propertyKeys, classType, ref);
        addClassReferenceSearchClauses(ctx, classType, ref);
        ctx.select(PrimaryKeyQueryContext.GREMLIN_STEP_RESULT);
        String gremlinQuery = ctx.buildQuery();
        LOG.debug("Searching for vertex by primary key with gremlin {} ", gremlinQuery);
        Vertex vertex = graphHelper.searchByGremlin(gremlinQuery, PrimaryKeyQueryContext.GREMLIN_STEP_RESULT);
        if (vertex != null) {
            //Check for array of classes property matches
            if (ctx.hasArrayRefInPrimaryKey()) {
                return checkArrayReferences(ctx, vertex, classType, ref);
            } else {
                return vertex;
            }
        }
        return vertex;
    }

    Vertex checkArrayReferences(PrimaryKeyQueryContext ctx, Vertex vertex, ClassType clsType, IReferenceableInstance ref) throws AtlasException {
        for (AttributeInfo arrInfo : ctx.getArrReferences()) {
            String arrEdgeLabel = GraphHelper.getEdgeLabel(clsType, arrInfo);
            final Iterable<Edge> edges = vertex.getEdges(Direction.OUT, arrEdgeLabel);
            Collection<Id> existingIds = new ArrayList<>();
            Collection<Id> currElements = (List<Id>) ref.get(arrInfo.name);

            for (Edge edge : edges) {
                Vertex inVertex = edge.getVertex(Direction.IN);
                String guid = inVertex.getProperty(Constants.GUID_PROPERTY_KEY);
                Id existingId = new Id(guid, 0, (String) inVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY),
                    (String) inVertex.getProperty(Constants.STATE_PROPERTY_KEY));
                existingIds.add(existingId);
            }
            if (existingIds.size() != currElements.size()) {
                return null;
            } else if (!currElements.equals(existingIds)) {
                return null;
            }
        }
        return vertex;
    }

    void addClassReferenceSearchClauses(PrimaryKeyQueryContext ctx, final ClassType classType, IReferenceableInstance ref) throws AtlasException {
        //Add all class reference searches to gremlin
        boolean addBackRef = false;
        List<AttributeInfo> classReferences = ctx.getClassReferences();
        if (classReferences != null) {
            for (AttributeInfo aInfo : classReferences) {
                Vertex classVertex = mapper.getClassVertex((IReferenceableInstance) ref.get(aInfo.name));
                String typeName = classVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
                String guid = classVertex.getProperty(Constants.GUID_PROPERTY_KEY);
                if (addBackRef) {
                    ctx.back(getFormattedString(PrimaryKeyQueryContext.GREMLIN_STEP_RESULT));
                }

                //Take the out edge label and check if the referred class has the following attributes
                ctx.out(classType, aInfo)
                    .has(Constants.GUID_PROPERTY_KEY, guid)
                    .has(Constants.ENTITY_TYPE_PROPERTY_KEY, typeName)
                    .has(Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());

                addBackRef = true;
            }
        }
    }

    PrimaryKeyQueryContext addSearchClauses(List<String> propertyKeys, ClassType classType, IReferenceableInstance ref) throws AtlasException {
        PrimaryKeyQueryContext gremlinCtx = new PrimaryKeyQueryContext();
        List<AttributeInfo> classReferences = null;
        List<AttributeInfo> arrReferences = null;

        for (final String property : propertyKeys) {
            AttributeInfo attrInfo = classType.fieldMapping().fields.get(property);
            String propertyQFName = getQualifiedFieldName(classType, attrInfo.name);
            if (attrInfo == null) {
                throw new IllegalArgumentException("Could not find property " + property + " in type " + classType.name);
            }
            final IDataType dataType = attrInfo.dataType();
            switch (dataType.getTypeCategory()) {
            case ENUM:
                gremlinCtx.has(propertyQFName, getFormattedString(ref.get(property)));
                break;
            case PRIMITIVE:
                gremlinCtx.has(propertyQFName, ref.get(property));
                break;
            case CLASS:
                if (classReferences == null) {
                    classReferences = new ArrayList<>();
                    gremlinCtx.setClassReferences(classReferences);
                }
                classReferences.add(attrInfo);
                break;
            case ARRAY:
                //Only process if array of classes.
                DataTypes.ArrayType arrType = (DataTypes.ArrayType) dataType;
                if ( arrType.getElemType().getTypeCategory() == DataTypes.TypeCategory.CLASS) {
                    if (arrReferences == null) {
                        arrReferences = new ArrayList<>();
                        gremlinCtx.setArrReferences(arrReferences);
                    }
                    arrReferences.add(attrInfo);
                } else if ( arrType.getElemType().getTypeCategory() == DataTypes.TypeCategory.PRIMITIVE ||
                    arrType.getElemType().getTypeCategory() == DataTypes.TypeCategory.ENUM ) {
                    List elements = (List) ref.get(property);
                    if ( elements != null && elements.size() > 0) {
                        gremlinCtx.has(propertyQFName, "T.eq", getFormattedString(elements));
                    }
                }
                break;
            //Maps, struct, trait are not supported
            default:
                throw new UnsupportedOperationException("Primary key having attribute of type " + dataType.getTypeCategory().name() + " is not supported");
            }
        }

        //Should be an active entity
        gremlinCtx.has(Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());
        //Add clause for typeName
        gremlinCtx.typeName(ref).alias(getFormattedString(PrimaryKeyQueryContext.GREMLIN_STEP_RESULT));

        return gremlinCtx;
    }

    private class PrimaryKeyQueryContext {

        public static final String GREMLIN_EDGE_LABEL_FMT = ".out('%s')";
        public static final String GREMLIN_SELECT_FMT = ".select([\"%s\"])";
        public static final String GREMLIN_STEP_RESULT = "result";
        public static final String GREMLIN_PROPERTY_SEARCH_FMT = ".has('%s', %s)";
        public static final String GREMLIN_PROPERTY_PRED_SEARCH_FMT = ".has('%s', '%s',  %s)";
        public static final String GREMLIN_ALIAS_FMT = ".as(%s)";
        public static final String GREMLIN_REFER_STEP_FMT = ".back(%s)";

        public final StringBuilder gremlinQuery = new StringBuilder();
        private List<AttributeInfo> classReferences;
        private List<AttributeInfo> arrReferences;

        public StringBuilder getGremlinQuery() {
            return gremlinQuery;
        }

        public List<AttributeInfo> getClassReferences() {
            return classReferences;
        }

        public void setClassReferences(final List<AttributeInfo> classReferences) {
            this.classReferences = classReferences;
        }

        public List<AttributeInfo> getArrReferences() {
            return arrReferences;
        }

        public void setArrReferences(final List<AttributeInfo> arrReferences) {
            this.arrReferences = arrReferences;
        }

        public boolean hasArrayRefInPrimaryKey() {
            return arrReferences != null;
        }

        public PrimaryKeyQueryContext typeName(IReferenceableInstance  instance) {
            has(Constants.ENTITY_TYPE_PROPERTY_KEY, instance.getTypeName());
            return this;
        }

        public PrimaryKeyQueryContext alias(String alias) {
            gremlinQuery.append(String.format(GREMLIN_ALIAS_FMT, alias));
            return this;
        }

        public PrimaryKeyQueryContext has(String property, Object value) {
            if (value instanceof String || value instanceof List) {
                value = getFormattedString(value);
            }
            gremlinQuery.append(String.format(PrimaryKeyQueryContext.GREMLIN_PROPERTY_SEARCH_FMT, property, value));
            return this;
        }

        public PrimaryKeyQueryContext has(String property, String predicate, Object value) {
            gremlinQuery.append(String.format(PrimaryKeyQueryContext.GREMLIN_PROPERTY_PRED_SEARCH_FMT, property, predicate, value));
            return this;
        }

        public PrimaryKeyQueryContext back(String step) {
            gremlinQuery.append(String.format(GREMLIN_REFER_STEP_FMT, step));
            return this;
        }

        public PrimaryKeyQueryContext out(IDataType dataType, AttributeInfo aInfo) throws AtlasException {
            gremlinQuery.append(String.format(PrimaryKeyQueryContext.GREMLIN_EDGE_LABEL_FMT, GraphHelper.getEdgeLabel(dataType, aInfo)));
            return this;
        }

        public String buildQuery() {
            return "g.V" + gremlinQuery + ".toList()";
        }

        public PrimaryKeyQueryContext select(String step) {
            gremlinQuery.append(String.format(PrimaryKeyQueryContext.GREMLIN_SELECT_FMT, step));
            return this;
        }
    }

    String getFormattedString(Object attrVal) {
        return "'" + String.valueOf(attrVal) + "'";
    }

    String getFormattedString(List elements) {
        return "['" + Joiner.on("','").join(elements) + "']";
    }

}
