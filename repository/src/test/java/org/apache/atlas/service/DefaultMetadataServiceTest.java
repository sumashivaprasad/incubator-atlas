/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.service;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.TypeNotFoundException;
import org.apache.atlas.repository.EntityNotFoundException;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.commons.lang.RandomStringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Guice(modules = RepositoryMetadataModule.class)
public class DefaultMetadataServiceTest {
    @Inject
    private MetadataService metadataService;
    @Inject
    private GraphProvider<TitanGraph> graphProvider;


    @BeforeClass
    public void setUp() throws Exception {
        TypesDef typesDef = TestUtils.defineHiveTypes();
        try {
            metadataService.getTypeDefinition(TestUtils.TABLE_TYPE);
        } catch (TypeNotFoundException e) {
            metadataService.createType(TypesSerialization.toJson(typesDef));
        }
    }

    @AfterClass
    public void shutdown() {
        try {
            //TODO - Fix failure during shutdown while using BDB
            graphProvider.get().shutdown();
        } catch(Exception e) {
            e.printStackTrace();
        }
        try {
            TitanCleanup.clear(graphProvider.get());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private String createInstance(Referenceable entity) throws Exception {
        String entityjson = InstanceSerialization.toJson(entity, true);
        JSONArray entitiesJson = new JSONArray();
        entitiesJson.put(entityjson);
        String response = metadataService.createOrUpdateEntities(entitiesJson.toString());
        return new JSONArray(response).getString(0);
    }

    private void updateInstance(String guid, String property, Object value) throws Exception {
        String attrValueJson = InstanceSerialization._toJson(value, true);
        metadataService.updateEntity(guid, property, attrValueJson);
    }

    private Referenceable createDBEntity() {
        Referenceable entity = new Referenceable(TestUtils.DATABASE_TYPE);
        String dbName = RandomStringUtils.randomAlphanumeric(10);
        entity.set("name", dbName);
        entity.set("description", "us db");
        return entity;
    }

    private Referenceable createTableEntity(Referenceable db) {
        Referenceable entity = new Referenceable(TestUtils.TABLE_TYPE);
        String tableName = RandomStringUtils.randomAlphanumeric(10);
        entity.set("name", tableName);
        entity.set("description", "us db");
        entity.set("type", "type");
        entity.set("tableType", "MANAGED");
        entity.set("database", db);
        return entity;
    }

    @Test
    public void testCreateEntityWithUniqueAttribute() throws Exception {
        //name is the unique attribute
        Referenceable entity = createDBEntity();
        String id = createInstance(entity);

        //using the same name should succeed, but not create another entity
        String newId = createInstance(entity);
        Assert.assertEquals(newId, id);

        //Same entity, but different qualified name should succeed
        entity.set("name", TestUtils.randomString());
        newId = createInstance(entity);
        Assert.assertNotEquals(newId, id);
    }

    @Test
    public void testCreateEntityWithUniqueAttributeWithReference() throws Exception {
        Referenceable db = createDBEntity();
        String dbId = createInstance(db);

        Referenceable table = new Referenceable(TestUtils.TABLE_TYPE);
        table.set("name", TestUtils.randomString());
        table.set("description", "random table");
        table.set("type", "type");
        table.set("tableType", "MANAGED");
        table.set("database", db);
        createInstance(table);

        //table create should re-use the db instance created earlier
        String tableDefinitionJson =
                metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        Referenceable actualDb = (Referenceable) tableDefinition.get("database");
        Assert.assertEquals(actualDb.getId().id, dbId);
    }

    @Test
    public void testUpdateEntityWithArray() throws Exception {
        Referenceable db = createDBEntity();
        String id = createInstance(db);

        Referenceable table = createTableEntity(db);
        String tableId = createInstance(table);

        //Update entity, add new array attribute VALUE
        //LIST OF PRIMITIVES
        final List<String> colNameList = ImmutableList.of("col1", "col2");
        Referenceable tableUpdated = new Referenceable(TestUtils.TABLE_TYPE, new HashMap<String, Object>() {{ put("columnNames", colNameList); }});
        metadataService.updateEntity(tableId, tableUpdated);

        String tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        List<String> actualColumns = (List) tableDefinition.get("columnNames");
        Assert.assertEquals(actualColumns, colNameList);

        //LIST OF PRIMITIVES UPDATED
        final List<String> updatedColNameList = ImmutableList.of("col2", "col3");
        tableUpdated = new Referenceable(TestUtils.TABLE_TYPE, new HashMap<String, Object>() {{ put("columnNames", updatedColNameList); }});
        metadataService.updateEntity(tableId, tableUpdated);

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        actualColumns = (List) tableDefinition.get("columnNames");
        Assert.assertEquals(actualColumns, updatedColNameList);

        //test array of class with id
        final List<Referenceable> columns = new ArrayList<>();
        Map<String, Object> values = new HashMap<>();
        values.put("name", "col1");
        values.put("type", "type");
        Referenceable ref = new Referenceable("column_type", values);
        columns.add(ref);
        tableUpdated = new Referenceable(TestUtils.TABLE_TYPE, new HashMap<String, Object>() {{ put("columns", columns); }});
        metadataService.updateEntity(tableId, tableUpdated);

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        List<Referenceable> arrClsColumns = (List) tableDefinition.get("columns");
        Assert.assertTrue(arrClsColumns.get(0).equalsContents(columns.get(0)));

        //Remove an Id and insert another id
        values.clear();
        columns.clear();

        values.put("name", "col2");
        values.put("type", "type");
        columns.add(ref);
        tableUpdated = new Referenceable(TestUtils.TABLE_TYPE, new HashMap<String, Object>() {{ put("columns", columns); }});
        metadataService.updateEntity(tableId, tableUpdated);

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        arrClsColumns = (List) tableDefinition.get("columns");
        Assert.assertTrue(arrClsColumns.get(0).equalsContents(columns.get(0)));


        //Remove array value
        values.clear();
        columns.clear();

        values.put("name", "col2");
        values.put("type", "type");
        columns.add(ref);
        tableUpdated = new Referenceable(TestUtils.TABLE_TYPE, new HashMap<String, Object>() {{ put("columns", columns); }});
        metadataService.updateEntity(tableId, tableUpdated);

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        arrClsColumns = (List) tableDefinition.get("columns");
        Assert.assertTrue(arrClsColumns.get(0).equalsContents(columns.get(0)));

        ITypedReferenceableInstance instance =

    }

    @Test
    public void testGetEntityByUniqueAttribute() throws Exception {
        Referenceable entity = createDBEntity();
        createInstance(entity);

        //get entity by valid qualified name
        String entityJson = metadataService.getEntityDefinition(TestUtils.DATABASE_TYPE, "name",
                (String) entity.get("name"));
        Assert.assertNotNull(entityJson);
        Referenceable referenceable = InstanceSerialization.fromJsonReferenceable(entityJson, true);
        Assert.assertEquals(referenceable.get("name"), entity.get("name"));

        //get entity by invalid qualified name
        try {
            metadataService.getEntityDefinition(TestUtils.DATABASE_TYPE, "name", "random");
            Assert.fail("Expected EntityNotFoundException");
        } catch (EntityNotFoundException e) {
            //expected
        }

        //get entity by non-unique attribute
        try {
            metadataService.getEntityDefinition(TestUtils.DATABASE_TYPE, "description",
                    (String) entity.get("description"));
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            //expected
        }
    }
}
