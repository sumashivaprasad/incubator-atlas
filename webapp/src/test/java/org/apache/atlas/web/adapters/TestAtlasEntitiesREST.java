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
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.web.rest.EntitiesREST;
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
    private AtlasTypeDefStoreInitializer initializer;

    @Inject
    private EntitiesREST restResource;

    private List<String> createdGuids = new ArrayList<>();

    private AtlasEntity dbEntity;

    private AtlasEntity tableEntity;

    private List<AtlasEntity> columns;

    @BeforeClass
    public void setUp() throws Exception {
        AtlasTypesDef typesDef = initializer.initializeStore(typeStore, typeRegistry, );
        typeStore.createTypesDef(typesDef);

        dbEntity = TestUtilsV2.createDBEntity();
//        dbEntity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, "default");
//        dbEntity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, dbEntity.getAttribute(AtlasClient.NAME) + "@" + AtlasConstants.CLUSTER_NAME_ATTRIBUTE);

        tableEntity = TestUtilsV2.createTableEntity(dbEntity.getGuid());
//        tableEntity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tableEntity.getAttribute(AtlasClient.NAME) + "@" + AtlasConstants.CLUSTER_NAME_ATTRIBUTE);
        tableEntity.setAttribute("parametersMap", new java.util.HashMap<String, String>() {{
            put("key1", "value1");
        }});

        final AtlasEntity colEntity = TestUtilsV2.createColumnEntity();
//        colEntity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, colEntity.getAttribute(AtlasClient.NAME) + "@" + AtlasConstants.CLUSTER_NAME_ATTRIBUTE);
        colEntity.setAttribute("type", "VARCHAR(32)");
//        colEntity.setAttribute("table", tableEntity.getGuid());

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

        final EntityMutationResponse response = restResource.createOrUpdate(entities);
        List<AtlasEntityHeader> guids = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE_OR_UPDATE);
        Assert.assertEquals(guids.size(), 3);

        for (AtlasEntityHeader header : guids) {
            createdGuids.add(header.getGuid());
        }
    }

    @Test(dependsOnMethods = "testCreateOrUpdateEntities")
    public void testGetEntities() throws Exception {

        final AtlasEntity.AtlasEntities response = restResource.getById(createdGuids);
        final List<AtlasEntity> entities = response.getList();

        Assert.assertEquals(entities.size(), 3);
        verifyAttributes(entities);
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

        LOG.info("verifying entity of type {} ", dbEntity.getTypeName());
        verifyAttributes(dbEntity.getAttributes(), retrievedDBEntity.getAttributes());
        LOG.info("verifying entity of type {} ", columns.get(0).getTypeName());
        verifyAttributes(columns.get(0).getAttributes(), retrievedColumnEntity.getAttributes());

        Assert.assertEquals(tableEntity.getAttribute(AtlasClient.NAME), retrievedTableEntity.getAttribute(AtlasClient.NAME));
        Assert.assertEquals(tableEntity.getAttribute("parametersMap"), retrievedTableEntity.getAttribute("parametersMap"));
    }

    private void verifyAttributes(Map<String, Object> sourceAttrs, Map<String, Object> targetAttributes) throws Exception {
        for (String name : sourceAttrs.keySet() ) {
            LOG.info("verifying attribute {} ", name);
            Assert.assertEquals(targetAttributes.get(name), sourceAttrs.get(name));
        }
    }

}
