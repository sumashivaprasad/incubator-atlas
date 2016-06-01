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
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.discovery.DiscoveryException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.commons.lang.StringUtils;

import java.util.Iterator;
import java.util.List;

public class PrimaryKeyQueryContext {

    public static final String GREMLIN_EDGE_LABEL_FMT = ".out('%s')";
    public static final String GREMLIN_SELECT_FMT = ".select([\"%s\"])";
    public static final String GREMLIN_STEP_RESULT = "result";
    public static final String GREMLIN_PROPERTY_SEARCH_FMT = ".has('%s', %s)";
    public static final String GREMLIN_PROPERTY_PRED_SEARCH_FMT = ".has('%s', '%s',  %s)";
    public static final String GREMLIN_ALIAS_FMT = ".as(%s)";
    public static final String GREMLIN_REFER_STEP_FMT = ".back(%s)";

    private Iterator<Vertex> startVertices;

    public final StringBuilder gremlinQuery = new StringBuilder();
    private List<AttributeInfo> classReferences;
    private List<AttributeInfo> arrReferences;

    private static final GraphHelper graphHelper = GraphHelper.getInstance();

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
    }

    public PrimaryKeyQueryContext select(String step) {
        gremlinQuery.append(String.format(PrimaryKeyQueryContext.GREMLIN_SELECT_FMT, step));
        return this;
    }

    public static String getFormattedString(Object attrVal) {
        return "'" + String.valueOf(attrVal) + "'";
    }

    public static String getFormattedString(List elements) {
        return "['" + Joiner.on("','").join(elements) + "']";
    }

    public Vertex executeQuery(String gremlinQuery, String resultStep) throws DiscoveryException {
        return graphHelper.searchByGremlin(gremlinQuery, resultStep);
    }

    public Vertex executeQuery(String gremlinQuery) throws DiscoveryException {

        return graphHelper.searchByGremlin(gremlinQuery, GREMLIN_STEP_RESULT);
    }

    public void start(Iterator<Vertex> vertices) {
        startVertices = vertices;
    }

    public Iterator<Vertex> startVertices() {
        return startVertices;
    }
}

