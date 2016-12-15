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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;

import org.apache.atlas.AtlasException;
import org.apache.atlas.discovery.graph.DefaultGraphPersistenceStrategy;
import org.apache.atlas.gremlin.Gremlin3ExpressionFactory;
import org.apache.atlas.gremlin.GremlinExpressionFactory;
import org.apache.atlas.gremlin.optimizer.GremlinQueryOptimizer;
import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.IdentifierExpression;
import org.apache.atlas.groovy.LiteralExpression;
import org.apache.atlas.groovy.TraversalStepType;
import org.apache.atlas.query.GraphPersistenceStrategies;
import org.apache.atlas.query.TypeUtils.FieldInfo;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.GremlinVersion;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test
public class GremlinQueryOptimizerTest implements IAtlasGraphProvider {

    public static final GremlinExpressionFactory FACTORY = new Gremlin3ExpressionFactory();
    
    private MetadataRepository repo = new GraphBackedMetadataRepository(this, new HardDeleteHandler(TypeSystem.getInstance()));
    private final GraphPersistenceStrategies STRATEGY = new DefaultGraphPersistenceStrategy(repo);
    @BeforeClass
    public void setUp() {
        GremlinQueryOptimizer.reset();
        GremlinQueryOptimizer.setExpressionFactory(FACTORY);
    }

    private FieldInfo getTestFieldInfo() throws AtlasException {
        AttributeDefinition def = new AttributeDefinition("foo", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null);
        AttributeInfo attrInfo = new AttributeInfo(TypeSystem.getInstance(), def, null);
        return new FieldInfo(DataTypes.STRING_TYPE, attrInfo, null, null);
    }

    private GroovyExpression getVerticesExpression() {
        IdentifierExpression g = new IdentifierExpression("g");
        return new FunctionCallExpression(TraversalStepType.START, g, "V");
    }
    
    
    @Test
    public void testPullHasExpressionsOutOfAnd() throws AtlasException {
        
        GroovyExpression expr1 = makeOutExpression(null, "out1");
        GroovyExpression expr2 = makeOutExpression(null, "out2");        
        GroovyExpression expr3 = makeHasExpression("prop1","Fred");
        GroovyExpression expr4 = makeHasExpression("prop2","George");
        GroovyExpression toOptimize = FACTORY.generateLogicalExpression(getVerticesExpression(), "and", Arrays.asList(expr1, expr2, expr3, expr4));
        
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), "g.V().has('prop1',eq('Fred')).has('prop2',eq('George')).and(out('out1'),out('out2'))");
    }
    
    @Test
    public void testOrGrouping() throws AtlasException {
        GroovyExpression expr1 = makeOutExpression(null, "out1");
        GroovyExpression expr2 = makeOutExpression(null, "out2");
        GroovyExpression expr3 = makeHasExpression("prop1","Fred");
        GroovyExpression expr4 = makeHasExpression("prop2","George");
        GroovyExpression toOptimize = FACTORY.generateLogicalExpression(getVerticesExpression(), "or", Arrays.asList(expr1, expr2, expr3, expr4));
        
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), "def r=(([]) as Set);g.V().has('prop1',eq('Fred')).fill(r);g.V().has('prop2',eq('George')).fill(r);g.V().or(out('out1'),out('out2')).fill(r);r");
    }

    
    @Test
    public void testAndOfOrs() throws AtlasException {
        
        GroovyExpression or1Cond1 = makeHasExpression("p1","e1");
        GroovyExpression or1Cond2 = makeHasExpression("p2","e2");
        GroovyExpression or2Cond1 = makeHasExpression("p3","e3");
        GroovyExpression or2Cond2 = makeHasExpression("p4","e4");
        
        GroovyExpression or1 = FACTORY.generateLogicalExpression(null, "or", Arrays.asList(or1Cond1, or1Cond2));
        GroovyExpression or2 = FACTORY.generateLogicalExpression(null, "or", Arrays.asList(or2Cond1, or2Cond2));
        GroovyExpression toOptimize  = FACTORY.generateLogicalExpression(getVerticesExpression(), "and", Arrays.asList(or1, or2));
        
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), "def r=(([]) as Set);"
                + "g.V().has('p1',eq('e1')).has('p3',eq('e3')).fill(r);"
                + "g.V().has('p1',eq('e1')).has('p4',eq('e4')).fill(r);"
                + "g.V().has('p2',eq('e2')).has('p3',eq('e3')).fill(r);"
                + "g.V().has('p2',eq('e2')).has('p4',eq('e4')).fill(r);"
                + "r");
        
    }
    
    @Test
    public void testAndWithMultiCallArguments() throws AtlasException {
        
        GroovyExpression cond1 = makeHasExpression("p1","e1");
        GroovyExpression cond2 = makeHasExpression(cond1, "p2","e2");
        GroovyExpression cond3 = makeHasExpression("p3","e3");
        GroovyExpression cond4 = makeHasExpression(cond3, "p4","e4");
        
        GroovyExpression toOptimize  = FACTORY.generateLogicalExpression(getVerticesExpression(), "and", Arrays.asList(cond2, cond4));
        
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), "g.V().has('p1',eq('e1')).has('p2',eq('e2')).has('p3',eq('e3')).has('p4',eq('e4'))");
    }

    @Test
    public void testOrOfAnds() throws AtlasException {
        
        GroovyExpression or1Cond1 = makeHasExpression("p1","e1");
        GroovyExpression or1Cond2 = makeHasExpression("p2","e2");
        GroovyExpression or2Cond1 = makeHasExpression("p3","e3");
        GroovyExpression or2Cond2 = makeHasExpression("p4","e4");
        
        GroovyExpression or1 = FACTORY.generateLogicalExpression(null, "and", Arrays.asList(or1Cond1, or1Cond2));
        GroovyExpression or2 = FACTORY.generateLogicalExpression(null, "and", Arrays.asList(or2Cond1, or2Cond2));
        GroovyExpression toOptimize  = FACTORY.generateLogicalExpression(getVerticesExpression(), "or", Arrays.asList(or1, or2));
        
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), "def r=(([]) as Set);"
                + "g.V().has('p1',eq('e1')).has('p2',eq('e2')).fill(r);"
                + "g.V().has('p3',eq('e3')).has('p4',eq('e4')).fill(r);"
                + "r");
    }
    
    @Test
    public void testHasNotMovedToResult() throws AtlasException {
        GroovyExpression toOptimize = getVerticesExpression();
        GroovyExpression or1Cond1 = makeHasExpression("p1","e1");
        GroovyExpression or1Cond2 = makeHasExpression("p2","e2");     
        
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(or1Cond1, or1Cond2));
        toOptimize = makeHasExpression(toOptimize, "p3","e3");
        toOptimize = FACTORY.generateAliasExpression(toOptimize, "_src");
        toOptimize = FACTORY.generateSelectExpression(toOptimize, Collections.singletonList(new LiteralExpression("src1")), Collections.<GroovyExpression>singletonList(new IdentifierExpression("it")));
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), 
                "def r=(([]) as Set);"
                + "def f1={GraphTraversal x->x.has('p3',eq('e3')).as('_src').select('_src').fill(r)};"
                + "f1(g.V().has('p1',eq('e1')));f1(g.V().has('p2',eq('e2')));"
                + "g.V('').inject(((r) as Vertex[])).as('_src').select('src1').by((({it}) as Function))");
    }

    @Test
    public void testLongStringEndingWithOr() throws AtlasException {
        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = makeHasExpression(toOptimize, "name","Fred");
        toOptimize = makeHasExpression(toOptimize, "age","13");      
        toOptimize = makeOutExpression(toOptimize, "livesIn");           
        toOptimize = makeHasExpression(toOptimize, "state","Massachusetts");
        
        GroovyExpression or1cond1 = makeHasExpression("p1", "e1");
        GroovyExpression or1cond2 = makeHasExpression("p2", "e2");
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(or1cond1, or1cond2));
        
        GroovyExpression or2cond1 = makeHasExpression("p3", "e3");
        GroovyExpression or2cond2 = makeHasExpression("p4", "e4");
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(or2cond1, or2cond2));
        toOptimize = makeHasExpression(toOptimize, "p5","e5");
        toOptimize = makeHasExpression(toOptimize, "p6","e6");
        GroovyExpression or3cond1 = makeHasExpression("p7", "e7");
        GroovyExpression or3cond2 = makeHasExpression("p8", "e8");
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(or3cond1, or3cond2));
        
        
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), 
                  "def r=(([]) as Set);"
                + "def f1={g.V().has('name',eq('Fred')).has('age',eq('13')).out('livesIn').has('state',eq('Massachusetts'))};"
                + "def f2={GraphTraversal x->x.has('p5',eq('e5')).has('p6',eq('e6'))};"
                + "f2(f1().has('p1',eq('e1')).has('p3',eq('e3'))).has('p7',eq('e7')).fill(r);"
                + "f2(f1().has('p1',eq('e1')).has('p3',eq('e3'))).has('p8',eq('e8')).fill(r);"
                + "f2(f1().has('p1',eq('e1')).has('p4',eq('e4'))).has('p7',eq('e7')).fill(r);"
                + "f2(f1().has('p1',eq('e1')).has('p4',eq('e4'))).has('p8',eq('e8')).fill(r);"
                + "f2(f1().has('p2',eq('e2')).has('p3',eq('e3'))).has('p7',eq('e7')).fill(r);"
                + "f2(f1().has('p2',eq('e2')).has('p3',eq('e3'))).has('p8',eq('e8')).fill(r);"
                + "f2(f1().has('p2',eq('e2')).has('p4',eq('e4'))).has('p7',eq('e7')).fill(r);"
                + "f2(f1().has('p2',eq('e2')).has('p4',eq('e4'))).has('p8',eq('e8')).fill(r);"
                + "r");
    }

    
    @Test
    public void testLongStringNotEndingWithOr() throws AtlasException {
        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = makeHasExpression(toOptimize, "name","Fred");
        toOptimize = makeHasExpression(toOptimize, "age","13");
        toOptimize = makeOutExpression(toOptimize, "livesIn");        
        toOptimize = makeHasExpression(toOptimize, "state","Massachusetts");
        
        GroovyExpression or1cond1 = makeHasExpression("p1", "e1");
        GroovyExpression or1cond2 = makeHasExpression("p2", "e2");
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(or1cond1, or1cond2));
        
        GroovyExpression or2cond1 = makeHasExpression("p3", "e3");
        GroovyExpression or2cond2 = makeHasExpression("p4", "e4");
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(or2cond1, or2cond2));
        toOptimize = makeHasExpression(toOptimize, "p5","e5");
        toOptimize = makeHasExpression(toOptimize, "p6","e6");
        GroovyExpression or3cond1 = makeHasExpression("p7", "e7");
        GroovyExpression or3cond2 = makeHasExpression("p8", "e8");
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(or3cond1, or3cond2));
        toOptimize = makeHasExpression(toOptimize, "p9","e9");
        
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), 
                "def r=(([]) as Set);"
                + "def f1={g.V().has('name',eq('Fred')).has('age',eq('13')).out('livesIn').has('state',eq('Massachusetts'))};"
                + "def f2={GraphTraversal x->x.has('p5',eq('e5')).has('p6',eq('e6'))};"
                + "def f3={GraphTraversal x->x.has('p9',eq('e9')).fill(r)};"
                + "f3(f2(f1().has('p1',eq('e1')).has('p3',eq('e3'))).has('p7',eq('e7')));"
                + "f3(f2(f1().has('p1',eq('e1')).has('p3',eq('e3'))).has('p8',eq('e8')));"
                + "f3(f2(f1().has('p1',eq('e1')).has('p4',eq('e4'))).has('p7',eq('e7')));"
                + "f3(f2(f1().has('p1',eq('e1')).has('p4',eq('e4'))).has('p8',eq('e8')));"
                + "f3(f2(f1().has('p2',eq('e2')).has('p3',eq('e3'))).has('p7',eq('e7')));"
                + "f3(f2(f1().has('p2',eq('e2')).has('p3',eq('e3'))).has('p8',eq('e8')));"
                + "f3(f2(f1().has('p2',eq('e2')).has('p4',eq('e4'))).has('p7',eq('e7')));"
                + "f3(f2(f1().has('p2',eq('e2')).has('p4',eq('e4'))).has('p8',eq('e8')));"
                + "r");
    }

    
    @Test
    public void testToListConversion() throws AtlasException {
        
        GroovyExpression expr1 = makeHasExpression("prop1","Fred");
        GroovyExpression expr2 = makeHasExpression("prop2","George");
        GroovyExpression toOptimize = FACTORY.generateLogicalExpression(getVerticesExpression(), "or", Arrays.asList(expr1, expr2));
        toOptimize = new FunctionCallExpression(TraversalStepType.END, toOptimize,"toList");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), 
                  "def r=(([]) as Set);"
                  + "g.V().has('prop1',eq('Fred')).fill(r);"
                  + "g.V().has('prop2',eq('George')).fill(r);"
                  + "g.V('').inject(((r) as Vertex[])).toList()");
    }
    
    @Test
    public void testToListWithExtraStuff() throws AtlasException {
        
        GroovyExpression expr1 = makeHasExpression("prop1","Fred");
        GroovyExpression expr2 = makeHasExpression("prop2","George");
        GroovyExpression toOptimize = FACTORY.generateLogicalExpression(getVerticesExpression(), "or", Arrays.asList(expr1, expr2));
        toOptimize = new FunctionCallExpression(TraversalStepType.END, toOptimize,"toList");
        toOptimize = new FunctionCallExpression(toOptimize,"size");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), 
                  "def r=(([]) as Set);"
                  + "g.V().has('prop1',eq('Fred')).fill(r);"
                  + "g.V().has('prop2',eq('George')).fill(r);"
                  + "g.V('').inject(((r) as Vertex[])).toList().size()");
    }

    public void testAddClosureWithExitExpressionDifferentFromExpr() throws AtlasException {
        
        GroovyExpression expr1 = makeHasExpression("prop1","Fred");
        GroovyExpression expr2 = makeHasExpression("prop2","George");
        GroovyExpression toOptimize = FACTORY.generateLogicalExpression(getVerticesExpression(), "or", Arrays.asList(expr1, expr2));
        toOptimize = makeOutExpression(toOptimize, "knows");
        toOptimize = makeOutExpression(toOptimize, "livesIn");
        toOptimize = new FunctionCallExpression(TraversalStepType.END, toOptimize,"toList");
        toOptimize = new FunctionCallExpression(toOptimize,"size");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), 
                "def r=(([]) as Set);"
                + "g.V().has('prop1',eq('Fred')).out('knows').out('livesIn').fill(r);"
                + "g.V().has('prop2',eq('George')).out('knows').out('livesIn').fill(r);"
                + "g.V('').inject(((r) as Vertex[])).toList().size()");
        
    }
    
    @Test
    public void testAddClosureNoExitExpression() throws AtlasException {
        
        GroovyExpression expr1 = makeHasExpression("prop1","Fred");
        GroovyExpression expr2 = makeHasExpression("prop2","George");
        GroovyExpression toOptimize = FACTORY.generateLogicalExpression(getVerticesExpression(), "or", Arrays.asList(expr1, expr2));
        toOptimize = makeOutExpression(toOptimize, "knows");
        toOptimize = makeOutExpression(toOptimize, "livesIn");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), 
                  "def r=(([]) as Set);"
                + "g.V().has('prop1',eq('Fred')).out('knows').out('livesIn').fill(r);"
                + "g.V().has('prop2',eq('George')).out('knows').out('livesIn').fill(r);"
                + "r");
    }

    
    private GroovyExpression makeOutExpression(GroovyExpression parent, String label) {
        return FACTORY.generateAdjacentVerticesExpression(parent, AtlasEdgeDirection.OUT, label);
    }
    
    @Test
    public void testAddClosureWithExitExpressionEqualToExpr() throws AtlasException {
        
        GroovyExpression expr1 = makeHasExpression("prop1","Fred");
        GroovyExpression expr2 = makeHasExpression("prop2","George");
        GroovyExpression toOptimize = FACTORY.generateLogicalExpression(getVerticesExpression(), "or", Arrays.asList(expr1, expr2));
        
        toOptimize = makeOutExpression(toOptimize, "knows");
        toOptimize = makeOutExpression(toOptimize, "livesIn");
        toOptimize = new FunctionCallExpression(TraversalStepType.END, toOptimize,"toList");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(),
                "def r=(([]) as Set);"
                + "g.V().has('prop1',eq('Fred')).out('knows').out('livesIn').fill(r);"
                + "g.V().has('prop2',eq('George')).out('knows').out('livesIn').fill(r);"
                + "g.V('').inject(((r) as Vertex[])).toList()");
    }

    @Test
    public void testClosureNotCreatedWhenNoOrs() throws AtlasException {
        
        GroovyExpression expr1 = makeHasExpression("prop1","Fred");
        GroovyExpression expr2 = makeHasExpression("prop2","George");
        GroovyExpression toOptimize = FACTORY.generateLogicalExpression(getVerticesExpression(), "and", Arrays.asList(expr1, expr2));
        toOptimize = makeOutExpression(toOptimize, "knows");
        toOptimize = makeOutExpression(toOptimize, "livesIn");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), "g.V().has('prop1',eq('Fred')).has('prop2',eq('George')).out('knows').out('livesIn')");
    }
    

    
    private GroovyExpression makeHasExpression(String name, String value) throws AtlasException {
        return makeHasExpression(null, name, value);
    }
    private GroovyExpression makeHasExpression(GroovyExpression parent, String name, String value) throws AtlasException {
        return FACTORY.generateHasExpression(STRATEGY, parent, name, "=", new LiteralExpression(value), getTestFieldInfo());
    }
    private GroovyExpression makeFieldExpression(GroovyExpression parent, String fieldName) throws AtlasException {
        return FACTORY.generateFieldExpression(parent, getTestFieldInfo(), fieldName, false);
    }
    
    @Test
    public void testOrFollowedByAnd() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression("name","George");
        GroovyExpression expr3 = makeHasExpression("age","13");
        GroovyExpression expr4 = makeHasExpression("age","14");

        GroovyExpression toOptimize = FACTORY.generateLogicalExpression(getVerticesExpression(), "or", Arrays.asList(expr1,expr2));
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "and", Arrays.asList(expr3, expr4));
        
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);        
        assertEquals(optimized.toString(), "def r=(([]) as Set);"
                + "def f1={GraphTraversal x->x.has('age',eq('13')).has('age',eq('14')).fill(r)};"
                + "f1(g.V().has('name',eq('Fred')));"
                + "f1(g.V().has('name',eq('George')));"
                + "r");
    }
    
    @Test
    public void testOrFollowedByOr() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression("name","George");
        GroovyExpression expr3 = makeHasExpression("age","13");
        GroovyExpression expr4 = makeHasExpression("age","14");

        GroovyExpression toOptimize = FACTORY.generateLogicalExpression(getVerticesExpression(), "or", Arrays.asList(expr1,expr2));
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(expr3, expr4));
        
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), "def r=(([]) as Set);"
                + "g.V().has('name',eq('Fred')).has('age',eq('13')).fill(r);"
                + "g.V().has('name',eq('Fred')).has('age',eq('14')).fill(r);"
                + "g.V().has('name',eq('George')).has('age',eq('13')).fill(r);"
                + "g.V().has('name',eq('George')).has('age',eq('14')).fill(r);"
                + "r");
    }
    
    @Test
    public void testMassiveOrExpansion() throws AtlasException {
        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = makeHasExpression(toOptimize, "h1","h2");
        toOptimize = makeHasExpression(toOptimize, "h3","h4");
        for(int i = 0; i < 5; i++) {
            GroovyExpression expr1 = makeHasExpression("p1" + i,"e1" + i);
            GroovyExpression expr2 = makeHasExpression("p2" + i,"e2" + i);
            toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(expr1,expr2));
            toOptimize = makeHasExpression(toOptimize, "ha" + i,"hb" + i);
            toOptimize = makeHasExpression(toOptimize, "hc" + i,"hd" + i);
        }
        toOptimize = makeHasExpression(toOptimize, "h5","h6");
        toOptimize = makeHasExpression(toOptimize, "h7","h8");
        
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), 
                  "def r=(([]) as Set);"
                  + "def f1={g.V().has('h1',eq('h2')).has('h3',eq('h4'))};"
                  + "def f2={GraphTraversal x->x.has('ha0',eq('hb0')).has('hc0',eq('hd0'))};"
                  + "def f3={GraphTraversal x->x.has('ha1',eq('hb1')).has('hc1',eq('hd1'))};"
                  + "def f4={GraphTraversal x->x.has('ha2',eq('hb2')).has('hc2',eq('hd2'))};"
                  + "def f5={GraphTraversal x->x.has('ha3',eq('hb3')).has('hc3',eq('hd3'))};"
                  + "def f6={GraphTraversal x->x.has('ha4',eq('hb4')).has('hc4',eq('hd4')).has('h5',eq('h6')).has('h7',eq('h8')).fill(r)};"
                  + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p11',eq('e11'))).has('p12',eq('e12'))).has('p13',eq('e13'))).has('p14',eq('e14')));"
                  + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p11',eq('e11'))).has('p12',eq('e12'))).has('p13',eq('e13'))).has('p24',eq('e24')));"
                  + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p11',eq('e11'))).has('p12',eq('e12'))).has('p23',eq('e23'))).has('p14',eq('e14')));"
                  + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p11',eq('e11'))).has('p12',eq('e12'))).has('p23',eq('e23'))).has('p24',eq('e24')));"
                  + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p11',eq('e11'))).has('p22',eq('e22'))).has('p13',eq('e13'))).has('p14',eq('e14')));"
                  + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p11',eq('e11'))).has('p22',eq('e22'))).has('p13',eq('e13'))).has('p24',eq('e24')));"
                  + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p11',eq('e11'))).has('p22',eq('e22'))).has('p23',eq('e23'))).has('p14',eq('e14')));"
                  + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p11',eq('e11'))).has('p22',eq('e22'))).has('p23',eq('e23'))).has('p24',eq('e24')));"
                  + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p21',eq('e21'))).has('p12',eq('e12'))).has('p13',eq('e13'))).has('p14',eq('e14')));"
                  + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p21',eq('e21'))).has('p12',eq('e12'))).has('p13',eq('e13'))).has('p24',eq('e24')));"
                  + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p21',eq('e21'))).has('p12',eq('e12'))).has('p23',eq('e23'))).has('p14',eq('e14')));"
                  + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p21',eq('e21'))).has('p12',eq('e12'))).has('p23',eq('e23'))).has('p24',eq('e24')));"
                  + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p21',eq('e21'))).has('p22',eq('e22'))).has('p13',eq('e13'))).has('p14',eq('e14')));"
                  + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p21',eq('e21'))).has('p22',eq('e22'))).has('p13',eq('e13'))).has('p24',eq('e24')));"
                  + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p21',eq('e21'))).has('p22',eq('e22'))).has('p23',eq('e23'))).has('p14',eq('e14')));"
                  + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p21',eq('e21'))).has('p22',eq('e22'))).has('p23',eq('e23'))).has('p24',eq('e24')));"
                  + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p11',eq('e11'))).has('p12',eq('e12'))).has('p13',eq('e13'))).has('p14',eq('e14')));"
                  + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p11',eq('e11'))).has('p12',eq('e12'))).has('p13',eq('e13'))).has('p24',eq('e24')));"
                  + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p11',eq('e11'))).has('p12',eq('e12'))).has('p23',eq('e23'))).has('p14',eq('e14')));"
                  + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p11',eq('e11'))).has('p12',eq('e12'))).has('p23',eq('e23'))).has('p24',eq('e24')));"
                  + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p11',eq('e11'))).has('p22',eq('e22'))).has('p13',eq('e13'))).has('p14',eq('e14')));"
                  + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p11',eq('e11'))).has('p22',eq('e22'))).has('p13',eq('e13'))).has('p24',eq('e24')));"
                  + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p11',eq('e11'))).has('p22',eq('e22'))).has('p23',eq('e23'))).has('p14',eq('e14')));"
                  + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p11',eq('e11'))).has('p22',eq('e22'))).has('p23',eq('e23'))).has('p24',eq('e24')));"
                  + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p21',eq('e21'))).has('p12',eq('e12'))).has('p13',eq('e13'))).has('p14',eq('e14')));"
                  + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p21',eq('e21'))).has('p12',eq('e12'))).has('p13',eq('e13'))).has('p24',eq('e24')));"
                  + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p21',eq('e21'))).has('p12',eq('e12'))).has('p23',eq('e23'))).has('p14',eq('e14')));"
                  + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p21',eq('e21'))).has('p12',eq('e12'))).has('p23',eq('e23'))).has('p24',eq('e24')));"
                  + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p21',eq('e21'))).has('p22',eq('e22'))).has('p13',eq('e13'))).has('p14',eq('e14')));"
                  + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p21',eq('e21'))).has('p22',eq('e22'))).has('p13',eq('e13'))).has('p24',eq('e24')));"
                  + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p21',eq('e21'))).has('p22',eq('e22'))).has('p23',eq('e23'))).has('p14',eq('e14')));"
                  + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p21',eq('e21'))).has('p22',eq('e22'))).has('p23',eq('e23'))).has('p24',eq('e24')));"
                  + "r");

    }
    
    public void testAndFollowedByAnd() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression("name","George");
        GroovyExpression expr3 = makeHasExpression("age","13");
        GroovyExpression expr4 = makeHasExpression("age","14");

        GroovyExpression toOptimize = FACTORY.generateLogicalExpression(getVerticesExpression(), "and", Arrays.asList(expr1,expr2));
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "and", Arrays.asList(expr3, expr4));
        
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), "g.V().has('name',eq('Fred')).has('name',eq('George')).has('age',eq('13')).has('age',eq('14'))");
        

    }

    @Test
    public void testAndFollowedByOr() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression("name","George");
        GroovyExpression expr3 = makeHasExpression("age","13");
        GroovyExpression expr4 = makeHasExpression("age","14");

        GroovyExpression toOptimize = FACTORY.generateLogicalExpression(getVerticesExpression(), "and", Arrays.asList(expr1,expr2));
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(expr3, expr4));
        
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), "def r=(([]) as Set);"
                + "def f1={g.V().has('name',eq('Fred')).has('name',eq('George'))};f1().has('age',eq('13')).fill(r);"
                + "f1().has('age',eq('14')).fill(r);"
                + "r");
    }
    
    @Test
    public void testInitialAlias() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression("name","George");


        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = FACTORY.generateAliasExpression(toOptimize, "x");
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(expr1, expr2));        
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), "def r=(([]) as Set);"
                + "g.V().as('x').has('name',eq('Fred')).fill(r);"
                + "g.V().as('x').has('name',eq('George')).fill(r);"
                + "r");
    }
    
    @Test
    public void testFinalAlias() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression("name","George");

        GroovyExpression toOptimize = getVerticesExpression();        
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(expr1, expr2));
        toOptimize = FACTORY.generateAliasExpression(toOptimize, "x");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), ""
                + "def r=(([]) as Set);"
                + "g.V().has('name',eq('Fred')).as('x').fill(r);"
                + "g.V().has('name',eq('George')).as('x').fill(r);"
                + "r");
    }
    
    @Test
    public void testAliasInMiddle() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression("name","George");
        GroovyExpression expr3 = makeHasExpression("age","13");
        GroovyExpression expr4 = makeHasExpression("age","14");


        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(expr1, expr2));
        toOptimize = FACTORY.generateAliasExpression(toOptimize, "x");
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(expr3, expr4));
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), 
                "def r=(([]) as Set);"
                + "g.V().has('name',eq('Fred')).as('x').has('age',eq('13')).fill(r);"
                + "g.V().has('name',eq('Fred')).as('x').has('age',eq('14')).fill(r);"
                + "g.V().has('name',eq('George')).as('x').has('age',eq('13')).fill(r);"
                + "g.V().has('name',eq('George')).as('x').has('age',eq('14')).fill(r);"
                + "r");
    }
    
    @Test
    public void testMultipleAliases() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression("name","George");
        GroovyExpression expr3 = makeHasExpression("age","13");
        GroovyExpression expr4 = makeHasExpression("age","14");


        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(expr1, expr2));
        toOptimize = FACTORY.generateAliasExpression(toOptimize, "x");
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(expr3, expr4));
        toOptimize = FACTORY.generateAliasExpression(toOptimize, "y");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), 
                  "def r=(([]) as Set);"
                + "def f1={GraphTraversal x->x.as('y').fill(r)};"
                + "f1(g.V().has('name',eq('Fred')).as('x').has('age',eq('13')));"
                + "f1(g.V().has('name',eq('Fred')).as('x').has('age',eq('14')));"
                + "f1(g.V().has('name',eq('George')).as('x').has('age',eq('13')));"
                + "f1(g.V().has('name',eq('George')).as('x').has('age',eq('14')));"
                + "r");
    }
    
    @Test
    public void testAliasInOrExpr() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = FACTORY.generateAliasExpression(makeHasExpression("name","George"), "george");
    
        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(expr1, expr2));
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), "def r=(([]) as Set);"
                + "g.V().has('name',eq('Fred')).fill(r);"
                + "g.V().or(has('name',eq('George')).as('george')).fill(r);"
                + "r");
    }
    
    @Test
    public void testAliasInAndExpr() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = FACTORY.generateAliasExpression(makeHasExpression("name","George"), "george");
    
        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "and", Arrays.asList(expr1, expr2));
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        //expression with alias cannot currently be pulled out of the and
        assertEquals(optimized.toString(), "g.V().has('name',eq('Fred')).and(has('name',eq('George')).as('george'))");
    }
    
    @Test
    public void testFlatMapExprInAnd() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression(makeOutExpression(null,"knows"), "name","George");
    
        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "and", Arrays.asList(expr1, expr2));
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), "g.V().has('name',eq('Fred')).and(out('knows').has('name',eq('George')))");
    }
    
    @Test
    public void testFlatMapExprInOr() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression(makeOutExpression(null,"knows"), "name","George");
    
        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(expr1, expr2));
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), 
                "def r=(([]) as Set);"
                + "g.V().has('name',eq('Fred')).fill(r);"
                + "g.V().or(out('knows').has('name',eq('George'))).fill(r);"
                + "r");
    }
    
    @Test
    public void testFieldExpressionPushedToResultExpression() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression(makeOutExpression(null,"knows"), "name","George");
    
        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(expr1, expr2));
        toOptimize = makeFieldExpression(toOptimize, "name");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), "def r=(([]) as Set);"
                + "g.V().has('name',eq('Fred')).fill(r);"
                + "g.V().or(out('knows').has('name',eq('George'))).fill(r);"
                + "g.V('').inject(((r) as Vertex[])).values('name')");
    }

    @Test
    public void testOrWithNoChildren() throws AtlasException {
        GroovyExpression toOptimize = getVerticesExpression();
        GroovyExpression expr1 = makeHasExpression(toOptimize, "name","Fred");
        
        toOptimize = FACTORY.generateLogicalExpression(expr1, "or", Collections.<GroovyExpression>emptyList());
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        //or with no children matches no vertices
        assertEquals(optimized.toString(), "def r=(([]) as Set);"
                + "r");
    }
    
    @Test
    public void testFinalAliasNeeded() throws AtlasException {
        GroovyExpression toOptimize = getVerticesExpression();        
        toOptimize = makeHasExpression(toOptimize, "name", "Fred");
        toOptimize = FACTORY.generateAliasExpression(toOptimize, "person");
        toOptimize = makeOutExpression(toOptimize, "livesIn");
        GroovyExpression isChicago = makeHasExpression(null, "name", "Chicago");
        GroovyExpression isBoston = makeHasExpression(null, "name", "Boston");
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(isChicago, isBoston));
        toOptimize = FACTORY.generateAliasExpression(toOptimize, "city");
        toOptimize = makeOutExpression(toOptimize, "state");
        toOptimize = makeHasExpression(toOptimize, "name", "Massachusetts");
        toOptimize = FACTORY.generatePathExpression(toOptimize);
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), 
                "def r=(([]) as Set);"
                + "def f1={g.V().has('name',eq('Fred')).as('person').out('livesIn')};"
                + "def f2={GraphTraversal x->x.as('city').out('state').has('name',eq('Massachusetts')).as('__res').select('person','city','__res').fill(r)};"
                + "f2(f1().has('name',eq('Chicago')));"
                + "f2(f1().has('name',eq('Boston')));"
                + "__(((r) as Map[])).as('__tmp').map({((Map)it.get()).get('person')}).as('person').select('__tmp').map({((Map)it.get()).get('city')}).as('city').select('__tmp').map({((Map)it.get()).get('__res')}).as('__res').path()");
    }

    
    
    @Test
    public void testRangeExpressionMovedToResult() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression(null, "name","Fred");
        GroovyExpression expr2 = makeHasExpression(null, "name","George");
        GroovyExpression expr3 = makeHasExpression(null, "age","34");
        GroovyExpression expr4 = makeHasExpression(null, "size","small");

        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "or", Arrays.asList(expr1, expr2));
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "and", Collections.singletonList(expr3));
        toOptimize = FACTORY.generateAdjacentVerticesExpression(toOptimize, AtlasEdgeDirection.OUT, "eats");
        toOptimize = FACTORY.generateLogicalExpression(toOptimize, "and", Collections.singletonList(expr4));
        toOptimize = makeHasExpression(toOptimize, "color","blue");
        toOptimize = FACTORY.generateLimitExpression(toOptimize, 0, 10);
        toOptimize = new FunctionCallExpression(TraversalStepType.END, toOptimize, "toList");
        toOptimize = new FunctionCallExpression(toOptimize, "size");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), 
                  "def r=(([]) as Set);"
                  + "def f1={GraphTraversal x->x.has('age',eq('34')).out('eats').has('size',eq('small')).has('color',eq('blue')).fill(r)};"
                  + "f1(g.V().has('name',eq('Fred')));f1(g.V().has('name',eq('George')));"
                  + "g.V('').inject(((r) as Vertex[])).range(0,10).toList().size()");
    }
    
    @Override
    public AtlasGraph get() throws RepositoryException {
        AtlasGraph graph = mock(AtlasGraph.class);
        when(graph.getSupportedGremlinVersion()).thenReturn(GremlinVersion.THREE);
        when(graph.isPropertyValueConversionNeeded(any(IDataType.class))).thenReturn(false);
        return graph;
    }
}
