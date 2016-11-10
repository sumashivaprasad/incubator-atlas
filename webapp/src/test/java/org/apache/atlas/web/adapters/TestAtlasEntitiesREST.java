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
package org.apache.atlas.web.adapters;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.web.rest.EntitiesREST;
import org.apache.atlas.web.rest.EntityREST;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Guice(modules = {AtlasFormatConvertersModule.class, RepositoryMetadataModule.class})
public class TestAtlasEntitiesREST {

    private static final Logger LOG = LoggerFactory.getLogger(TestAtlasEntitiesREST.class);

    @Inject
    private AtlasTypeDefStore typeStore;

    @Inject
    private AtlasTypeRegistry typeRegistry;

    @Inject
    private EntitiesREST entitiesREST;

    @Inject
    private EntityREST entityREST;

    private List<String> createdGuids = new ArrayList<>();

    private AtlasEntity dbEntity;

    private AtlasEntity tableEntity;

    private List<AtlasEntity> columns;

    @BeforeClass
    public void setUp() throws Exception {
        AtlasTypesDef typesDef = TestUtilsV2.defineHiveTypes();
        typeStore.createTypesDef(typesDef);
        dbEntity = TestUtilsV2.createDBEntity();

        tableEntity = TestUtilsV2.createTableEntity(dbEntity.getGuid());
        final AtlasEntity colEntity = TestUtilsV2.createColumnEntity();
        columns = new ArrayList<AtlasEntity>() {{ add(colEntity); }};
        tableEntity.setAttribute("columns", columns);
    }

    @AfterClass
    public void tearDown() throws Exception {
        AtlasGraphProvider.cleanup();
    }

    @Test
    public void testCreateOrUpdateEntities() throws Exception {
        List<AtlasEntity> entities = new ArrayList<AtlasEntity>();
        entities.add(dbEntity);
        entities.add(tableEntity);

        final EntityMutationResponse response = entitiesREST.createOrUpdate(entities);
        List<AtlasEntityHeader> guids = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE_OR_UPDATE);
        Assert.assertEquals(guids.size(), 3);

        for (AtlasEntityHeader header : guids) {
            createdGuids.add(header.getGuid());
        }
    }

    @Test(dependsOnMethods = "testCreateOrUpdateEntities")
    public void testGetEntities() throws Exception {

        final AtlasEntity.AtlasEntities response = entitiesREST.getById(createdGuids);
        final List<AtlasEntity> entities = response.getList();

        Assert.assertEquals(entities.size(), 3);
        verifyAttributes(entities);
    }

    @Test(dependsOnMethods = "testCreateOrUpdateEntities")
    public void testGetEntity() throws Exception {

         final AtlasEntity response = entityREST.getById(createdGuids.get(0));

         Assert.assertNotNull(response);
         verifyAttributes(new ArrayList<AtlasEntity>() {{ add(response); }});
    }

    @Test(dependsOnMethods = "testCreateOrUpdateEntities")
    public void testCreateOrUpdateEntity() throws Exception {

        dbEntity.setAttribute(TestUtilsV2.NAME, "updatedDBName");
        final EntityMutationResponse response = entityREST.createOrUpdate(dbEntity);

        Assert.assertNotNull(response);
        List<AtlasEntityHeader> entitiesMutated = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE_OR_UPDATE);
        Assert.assertEquals(entitiesMutated.size(), 1);
    }

    private void verifyAttributes(List<AtlasEntity> retrievedEntities) throws Exception {
        AtlasEntity retrievedDBEntity = null;
        AtlasEntity retrievedTableEntity = null;
        AtlasEntity retrievedColumnEntity = null;
        for (AtlasEntity entity:  retrievedEntities ) {
            if ( entity.getTypeName().equals(TestUtilsV2.DATABASE_TYPE)) {
                retrievedDBEntity = entity;
            }

            if ( entity.getTypeName().equals(TestUtilsV2.TABLE_TYPE)) {
                retrievedTableEntity = entity;
            }

            if ( entity.getTypeName().equals(TestUtilsV2.COLUMN_TYPE)) {
                retrievedColumnEntity = entity;
            }
        }

        if ( retrievedDBEntity != null) {
            LOG.info("verifying entity of type {} ", dbEntity.getTypeName());
            verifyAttributes(dbEntity.getAttributes(), retrievedDBEntity.getAttributes());
        }

        if ( retrievedColumnEntity != null) {
            LOG.info("verifying entity of type {} ", columns.get(0).getTypeName());
            verifyAttributes(columns.get(0).getAttributes(), retrievedColumnEntity.getAttributes());
        }

        if ( retrievedTableEntity != null) {
            LOG.info("verifying entity of type {} ", tableEntity.getTypeName());

            //String
            Assert.assertEquals(tableEntity.getAttribute(AtlasClient.NAME), retrievedTableEntity.getAttribute(AtlasClient.NAME));
            //Map
            Assert.assertEquals(tableEntity.getAttribute("parametersMap"), retrievedTableEntity.getAttribute("parametersMap"));
            //enum
            Assert.assertEquals(tableEntity.getAttribute("tableType"), retrievedTableEntity.getAttribute("tableType"));
            //date
            Assert.assertEquals(tableEntity.getAttribute("created"), retrievedTableEntity.getAttribute("created"));
            //array of Ids
            Assert.assertEquals(((List<AtlasEntity>) retrievedTableEntity.getAttribute("columns")).get(0).getGuid(), retrievedColumnEntity.getGuid());
            //array of structs
            Assert.assertEquals(((List<AtlasStruct>) retrievedTableEntity.getAttribute("partitions")), tableEntity.getAttribute("partitions"));
        }
    }

    private void verifyAttributes(Map<String, Object> sourceAttrs, Map<String, Object> targetAttributes) throws Exception {
        for (String name : sourceAttrs.keySet() ) {
            LOG.info("verifying attribute {} ", name);
            Assert.assertEquals(targetAttributes.get(name), sourceAttrs.get(name));
        }
    }



}
