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
import org.apache.atlas.discovery.DiscoveryException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

        PrimaryKeyQueryContext ctx = addPrimitiveSearchClauses(propertyKeys, classType, ref);

        List<Vertex> vertices = new ArrayList<>();
        if ( ctx.startVertices() == null || !ctx.startVertices().hasNext() ) {
            return null;
        } else if (ctx.getClassReferences() != null) {
            addClassReferenceSearchClauses(ctx, classType, ref);
            String gremlinQuery = ctx.buildQuery();
            LOG.debug("Searching for vertex by primary key with gremlin {} ", gremlinQuery);
            vertices = ctx.executeQuery(gremlinQuery);
        } else {
            vertices.add(ctx.startVertices().next());
        }


        if (vertices != null && vertices.size() > 0) {
            //Check for array of classes property matches
            if (ctx.hasArrayRefInPrimaryKey()) {
                return checkArrayReferences(ctx, vertices, classType, ref);
            }
        }

        if (vertices.size() > 1) {
            LOG.error("Found more than 1 vertex for the primary key {} {} {}", classType, propertyKeys, vertices);
            throw new IllegalStateException("Found more than 1 vertex for the primary key " + classType.getName() + ":" + propertyKeys + ":" + vertices);
        }

        return vertices.size() > 0 ? vertices.get(0) : null;


//        PrimaryKeyQueryContext ctx = addPrimitiveSearchClauses(propertyKeys, classType, ref);
//        addClassReferenceSearchClauses(ctx, classType, ref);
//        String gremlinQuery = ctx.buildQuery();
//        LOG.debug("Searching for vertex by primary key with gremlin {} ", gremlinQuery);
//        Vertex vertex = ctx.executeQuery(gremlinQuery);
//        if (vertex != null) {
//            //Check for array of classes property matches
//            if (ctx.hasArrayRefInPrimaryKey()) {
//                return checkArrayReferences(ctx, vertex, classType, ref);
//            } else {
//                return vertex;
//            }
//        }
//        return vertex;
    }

    Vertex checkArrayReferences(PrimaryKeyQueryContext ctx, List<Vertex> vertices, ClassType clsType, IReferenceableInstance ref) throws AtlasException {
        Vertex vertex = null;
        for(Vertex v : vertices) {
            for (AttributeInfo arrInfo : ctx.getArrReferences()) {
                String arrEdgeLabel = GraphHelper.getEdgeLabel(clsType, arrInfo);
                final Iterable<Edge> edges = v.getEdges(Direction.OUT, arrEdgeLabel);
                Collection<Id> existingIds = new ArrayList<>();
                Collection<Id> currElements = (List<Id>) ref.get(arrInfo.name);

                for (Edge edge : edges) {
                    Vertex inVertex = edge.getVertex(Direction.IN);
                    String guid = inVertex.getProperty(Constants.GUID_PROPERTY_KEY);
                    Id existingId = new Id(guid, 0, (String) inVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY),
                        (String) inVertex.getProperty(Constants.STATE_PROPERTY_KEY));
                    existingIds.add(existingId);
                }
                if (existingIds.size() == currElements.size() && currElements.equals(existingIds)) {
                   vertex = v;
                   break;
                }
            }
            if ( vertex != null ) {
                break;
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
                LOG.debug("Mapping class attribute for Primary key {} ", aInfo);
                Vertex classVertex = mapper.getClassVertex((IReferenceableInstance) ref.get(aInfo.name));
                if (classVertex == null) {
                    PrimaryKeyQueryContext pkc = new PrimaryKeyQueryContext();
                    classVertex = getClassVertex((IReferenceableInstance) ref.get(aInfo.name), pkc);
                }
                String typeName = classVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
                String guid = classVertex.getProperty(Constants.GUID_PROPERTY_KEY);
                if (addBackRef) {
                    ctx.back(getFormattedString(PrimaryKeyQueryContext.GREMLIN_STEP_RESULT));
                } else {
                    addBackRef = true;
                }

                //Take the out edge label and check if the referred class has the following attributes
                ctx.out(classType, aInfo)
                    .has(Constants.GUID_PROPERTY_KEY, guid)
                    .has(Constants.ENTITY_TYPE_PROPERTY_KEY, typeName)
                    .has(Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());
            }
        }
    }

    //For tests
    Vertex getClassVertex(final IReferenceableInstance instance, PrimaryKeyQueryContext pkc) throws AtlasException {
        String typeName = instance.getTypeName();
        ClassType clsType = typeSystem.getDataType(ClassType.class, typeName);

        for (AttributeInfo aInfo : clsType.getPrimaryKeyAttrs() ) {
            switch(aInfo.dataType().getTypeCategory()) {
            case PRIMITIVE:
            case ENUM:
                pkc.has(aInfo.name, instance.get(aInfo.name));
                break;
            case CLASS:
                pkc.back(PrimaryKeyQueryContext.GREMLIN_STEP_RESULT);
                pkc.out(clsType, aInfo);
                getClassVertex((IReferenceableInstance) instance.get(aInfo.name), pkc);
            }
        }

        pkc.select(PrimaryKeyQueryContext.GREMLIN_STEP_RESULT);
        String gremlin =  pkc.getGremlinQuery().toString();
        LOG.debug("gremlin query for class {} ", gremlin);

        List<Vertex> v = pkc.executeQuery(gremlin);
        if (  v != null &&  v.size() > 0) {
            return v.get(0);
        }
        return null;
    }

    PrimaryKeyQueryContext addPrimitiveSearchClauses(List<String> propertyKeys, ClassType classType, IReferenceableInstance ref) throws AtlasException {
        PrimaryKeyQueryContext gremlinCtx = new PrimaryKeyQueryContext();
        List<AttributeInfo> classReferences = null;
        List<AttributeInfo> arrReferences = null;

        Map<String, Object> vertexFilterKeys = new LinkedHashMap<>();
        boolean keyLookupIndexUsed = false;
        for (final String property : propertyKeys) {
            AttributeInfo attrInfo = classType.fieldMapping().fields.get(property);
            String propertyQFName = getQualifiedFieldName(classType, attrInfo.name);
            if (attrInfo == null) {
                throw new IllegalArgumentException("Could not find property " + property + " in type " + classType.name);
            }
            final IDataType dataType = attrInfo.dataType();
            switch (dataType.getTypeCategory()) {
            case ENUM:

                vertexFilterKeys.put(propertyQFName, getFormattedString(ref.get(property)));
//                if ( !keyLookupIndexUsed) {
//                    gremlinCtx.key(propertyQFName, getFormattedString(ref.get(property)));
//                    keyLookupIndexUsed = true;
//                } else {
//                    gremlinCtx.has(propertyQFName, getFormattedString(ref.get(property)));
//                }
                break;
            case PRIMITIVE:

//                if ( !keyLookupIndexUsed) {
//                    gremlinCtx.key(propertyQFName, String.valueOf(ref.get(property)));
//                    keyLookupIndexUsed = true;
//                } else {
//                    gremlinCtx.has(propertyQFName, String.valueOf(ref.get(property)));
//                }
                vertexFilterKeys.put(propertyQFName, ref.get(property));
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
//        gremlinCtx.has(Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());
        vertexFilterKeys.put(Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());
        //Add clause for typeName
//        gremlinCtx.has(Constants.ENTITY_TYPE_PROPERTY_KEY, ref.getTypeName());
        vertexFilterKeys.put(Constants.ENTITY_TYPE_PROPERTY_KEY, ref.getTypeName());

        try {
            Iterator<Vertex> startVertices = graphHelper.findVertices(vertexFilterKeys);

            if (startVertices != null && startVertices.hasNext()) {
                gremlinCtx.start(startVertices);
            }
        } catch(EntityNotFoundException enfe) {
            //Ignore
        }

        gremlinCtx.alias(PrimaryKeyQueryContext.GREMLIN_STEP_RESULT);
        return gremlinCtx;
    }

    private class PrimaryKeyQueryContext {

        public static final String GREMLIN_EDGE_LABEL_FMT = ".out('%s')";
        public static final String GREMLIN_SELECT_FMT = ".select([\"%s\"])";
        public static final String GREMLIN_STEP_RESULT = "result";
        public static final String GREMLIN_PROPERTY_SEARCH_FMT = ".has('%s', %s)";
        public static final String GREMLIN_PROPERTY_PRED_SEARCH_FMT = ".has('%s', '%s',  %s)";
        public static final String GREMLIN_ALIAS_FMT = ".as('%s')";
        public static final String GREMLIN_REFER_STEP_FMT = ".back(%s)";

        private Iterator<Vertex> startVertices;

        public final StringBuilder gremlinQuery = new StringBuilder();
        private List<AttributeInfo> classReferences;
        private List<AttributeInfo> arrReferences;

        private final GraphHelper graphHelper = GraphHelper.getInstance();

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

        public PrimaryKeyQueryContext typeName(String typeName) {
            has(Constants.ENTITY_TYPE_PROPERTY_KEY, typeName);
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

        public PrimaryKeyQueryContext keyLookup(String property, String value) {
            gremlinQuery.append(String.format(".V(\"%s\",\"%s\")", property, value));
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

            select(GREMLIN_STEP_RESULT);
            StringBuilder startVertexIds = new StringBuilder();

            while (startVertices.hasNext()) {
                startVertexIds.append(startVertices.next().getId());
                if (startVertices.hasNext()) {
                    startVertexIds.append(",");
                }
            }
            return String.format("g.v(%s).as('%s')" + gremlinQuery + ".toList()", startVertexIds.toString(), GREMLIN_STEP_RESULT);
//            return String.format("g%s.toList()", gremlinQuery);
        }

        public PrimaryKeyQueryContext select(String step) {
            gremlinQuery.append(String.format(PrimaryKeyQueryContext.GREMLIN_SELECT_FMT, step));
            return this;
        }

        public List<Vertex> executeQuery(String gremlinQuery, String resultStep) throws DiscoveryException {
            return graphHelper.searchByGremlin(gremlinQuery, resultStep);
        }

        public List<Vertex> executeQuery(String gremlinQuery) throws DiscoveryException {
            return graphHelper.searchByGremlin(gremlinQuery, GREMLIN_STEP_RESULT);
        }

        public void start(Iterator<Vertex> vertices) {
            startVertices = vertices;
        }

        public Iterator<Vertex> startVertices() {
            return startVertices;
        }
    }

    public static String getFormattedString(Object attrVal) {
        return "'" + String.valueOf(attrVal) + "'";
    }

    public static String getFormattedString(List elements) {
        return "['" + Joiner.on("','").join(elements) + "']";
    }
}
