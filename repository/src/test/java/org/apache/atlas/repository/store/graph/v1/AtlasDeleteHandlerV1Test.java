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
package org.apache.atlas.repository.store.graph.v1;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.TestUtils;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.TestUtils.COLUMNS_ATTR_NAME;
import static org.apache.atlas.TestUtils.COLUMN_TYPE;
import static org.apache.atlas.TestUtils.NAME;
import static org.apache.atlas.TestUtils.TABLE_TYPE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Guice(modules = RepositoryMetadataModule.class)
public abstract class AtlasDeleteHandlerV1Test {

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    AtlasTypeDefStore typeDefStore;

    AtlasEntityStore entityStore;

    @Inject
    MetadataService metadataService;

    private AtlasEntityType compositeMapOwnerType;

    private AtlasEntityType compositeMapValueType;


    @BeforeClass
    public void setUp() throws Exception {
        metadataService = TestUtils.addSessionCleanupWrapper(metadataService);
        new GraphBackedSearchIndexer(typeRegistry);
        final AtlasTypesDef deptTypesDef = TestUtilsV2.defineDeptEmployeeTypes();
        typeDefStore.createTypesDef(deptTypesDef);

        final AtlasTypesDef hiveTypesDef = TestUtilsV2.defineHiveTypes();
        typeDefStore.createTypesDef(hiveTypesDef);

        // Define type for map value.
        AtlasEntityDef mapValueDef = AtlasTypeUtil.createClassTypeDef("CompositeMapValue", "CompositeMapValue" + "_description", "1.0",
            ImmutableSet.<String>of(),
            AtlasTypeUtil.createUniqueRequiredAttrDef("name", "string")
        );

        // Define type with map where the value is a composite class reference to MapValue.
        AtlasEntityDef mapOwnerDef = AtlasTypeUtil.createClassTypeDef("CompositeMapOwner", "CompositeMapOwner_description",
            ImmutableSet.<String>of(),
            AtlasTypeUtil.createUniqueRequiredAttrDef("name", "string"),
            AtlasTypeUtil.createOptionalAttrDef("map", "map<string,string>")
        );

        final AtlasTypesDef typesDef = AtlasTypeUtil.getTypesDef(ImmutableList.<AtlasEnumDef>of(),
            ImmutableList.<AtlasStructDef>of(),
            ImmutableList.<AtlasClassificationDef>of(),
            ImmutableList.of(mapValueDef, mapOwnerDef));

        typeDefStore.createTypesDef(typesDef);

        compositeMapOwnerType = typeRegistry.getEntityTypeByName("CompositeMapOwner");
        compositeMapValueType = typeRegistry.getEntityTypeByName("CompositeMapValue");
    }

    @BeforeTest
    public void init() throws Exception {

        final Class<? extends DeleteHandlerV1> deleteHandlerImpl = AtlasRepositoryConfiguration.getDeleteHandlerV1Impl();
        final Constructor<? extends DeleteHandlerV1> deleteHandlerImplConstructor = deleteHandlerImpl.getConstructor(AtlasTypeRegistry.class);
        DeleteHandlerV1 deleteHandler = deleteHandlerImplConstructor.newInstance(typeRegistry);
        ArrayVertexMapper arrVertexMapper = new ArrayVertexMapper(deleteHandler);
        MapVertexMapper mapVertexMapper = new MapVertexMapper(deleteHandler);

        entityStore = new AtlasEntityStoreV1(new EntityGraphMapper(arrVertexMapper, mapVertexMapper, deleteHandler), deleteHandler);
        entityStore.init(typeRegistry);

        RequestContextV1.clear();
    }

    @AfterClass
    public void clear() {
        AtlasGraphProvider.cleanup();
    }

    abstract DeleteHandlerV1 getDeleteHandler(AtlasTypeRegistry typeRegistry);

    @Test
    public void testDeleteAndCreate() throws Exception {
        final Map<String, AtlasEntity> dbEntity = TestUtilsV2.createDBEntity();
        EntityMutationResponse response = entityStore.createOrUpdate(dbEntity);

        //delete entity should mark it as deleted
        EntityMutationResponse deleteResponse = entityStore.deleteById(response.getFirstEntityCreated().getGuid());
        assertEquals(deleteResponse.getEntitiesByOperation(EntityMutations.EntityOperation.DELETE).get(0).getGuid(), response.getFirstEntityCreated().getGuid());

        //TODO - Enable after GET API is ready
//        //get entity by unique attribute should throw EntityNotFoundException
//        try {
//            repositoryService.getEntityDefinition(TestUtils.DATABASE_TYPE, "name", entity.get("name"));
//            fail("Expected EntityNotFoundException");
//        } catch(EntityNotFoundException e) {
//            //expected
//        }

        //Create the same entity again, should create new entity
        EntityMutationResponse newCreationResponse = entityStore.createOrUpdate(dbEntity);
        assertNotEquals(newCreationResponse.getFirstEntityCreated().getGuid(), response.getFirstEntityCreated().getGuid());

        //TODO - Enable after GET is ready
//        //get by unique attribute should return the new entity
//        instance = repositoryService.getEntityDefinition(TestUtils.DATABASE_TYPE, "name", entity.get("name"));
//        assertEquals(instance.getId()._getId(), newId);
    }

    @Test
    public void testDeleteReference() throws Exception {
        //Deleting column should update table
        final Map<String, AtlasEntity> dbEntityMap = TestUtilsV2.createDBEntity();

        init();
        EntityMutationResponse dbCreationResponse = entityStore.createOrUpdate(dbEntityMap);

        final Map<String, AtlasEntity> tableEntityMap = TestUtilsV2.createTableEntity(dbCreationResponse.getFirstEntityCreated().getGuid());
        final AtlasEntity columnEntity = TestUtilsV2.createColumnEntity(getFirstGuid(tableEntityMap));
        AtlasEntity tableEntity = tableEntityMap.values().iterator().next();
        tableEntity.setAttribute(COLUMNS_ATTR_NAME, Arrays.asList(columnEntity.getAtlasObjectId()));
        tableEntityMap.put(columnEntity.getGuid(), columnEntity);

        init();
        EntityMutationResponse tblCreationResponse = entityStore.createOrUpdate(tableEntityMap);
        final AtlasEntityHeader columnCreated = tblCreationResponse.getFirstCreatedEntityByTypeName(COLUMN_TYPE);
        final AtlasEntityHeader tableCreated = tblCreationResponse.getFirstCreatedEntityByTypeName(TABLE_TYPE);

        init();
        EntityMutationResponse deletionResponse = entityStore.deleteById(columnCreated.getGuid());
        assertEquals(deletionResponse.getDeletedEntities().size(), 1);
        assertEquals(deletionResponse.getDeletedEntities().get(0).getGuid(), columnCreated.getGuid());
        assertEquals(deletionResponse.getUpdatedEntities().size(), 1);
        assertEquals(deletionResponse.getUpdatedEntities().get(0).getGuid(), tableCreated.getGuid());

        assertEntityDeleted(columnCreated.getGuid());

        //TODO - Fix after GET is ready
//        ITypedReferenceableInstance tableInstance = repositoryService.getEntityDefinition(tableId);
//        assertColumnForTestDeleteReference(tableInstance);

        //Deleting table should update process
        Map<String, AtlasEntity> processMap = TestUtilsV2.createProcessEntity(null, Arrays.asList(tableCreated.getAtlasObjectId()));
        init();
        final EntityMutationResponse processCreationResponse = entityStore.createOrUpdate(processMap);

        init();
        entityStore.deleteById(tableCreated.getGuid());
        assertEntityDeleted(tableCreated.getGuid());

        assertTableForTestDeleteReference(tableCreated.getGuid());
        assertProcessForTestDeleteReference(processCreationResponse.getFirstEntityCreated());
    }

    @Test
    public void testDeleteEntities() throws Exception {
        // Create a table entity, with 3 composite column entities
        init();
        final Map<String, AtlasEntity> dbEntityMap = TestUtilsV2.createDBEntity();
        EntityMutationResponse dbCreationResponse = entityStore.createOrUpdate(dbEntityMap);

        final Map<String, AtlasEntity> tableEntityMap = TestUtilsV2.createTableEntity(dbCreationResponse.getFirstEntityCreated().getGuid());

        AtlasEntity tableEntity = tableEntityMap.values().iterator().next();

        String tableGuid = getFirstGuid(tableEntityMap);
        final AtlasEntity columnEntity1 = TestUtilsV2.createColumnEntity(tableGuid);
        tableEntityMap.put(columnEntity1.getGuid(), columnEntity1);

        final AtlasEntity columnEntity2 = TestUtilsV2.createColumnEntity(tableGuid);
        tableEntityMap.put(columnEntity2.getGuid(), columnEntity2);

        final AtlasEntity columnEntity3 = TestUtilsV2.createColumnEntity(tableGuid);
        tableEntityMap.put(columnEntity3.getGuid(), columnEntity3);

        tableEntity.setAttribute(COLUMNS_ATTR_NAME, Arrays.asList(columnEntity1.getAtlasObjectId(), columnEntity2.getAtlasObjectId(), columnEntity3.getAtlasObjectId()));

        init();
        final EntityMutationResponse tblCreationResponse = entityStore.createOrUpdate(tableEntityMap);

        final AtlasEntityHeader column1Created = tblCreationResponse.getCreatedEntityByTypeNameAndAttribute(COLUMN_TYPE, NAME, (String) columnEntity1.getAttribute(NAME));
        final AtlasEntityHeader column2Created = tblCreationResponse.getCreatedEntityByTypeNameAndAttribute(COLUMN_TYPE, NAME, (String) columnEntity2.getAttribute(NAME));
        final AtlasEntityHeader column3Created = tblCreationResponse.getCreatedEntityByTypeNameAndAttribute(COLUMN_TYPE, NAME, (String) columnEntity3.getAttribute(NAME));

        // Retrieve the table entities from the Repository, to get their guids and the composite column guids.
        ITypedReferenceableInstance tableInstance = metadataService.getEntityDefinitionReference(TestUtils.TABLE_TYPE, NAME, (String) tableEntity.getAttribute(NAME));
        List<IReferenceableInstance> columns = (List<IReferenceableInstance>) tableInstance.get(COLUMNS_ATTR_NAME);

        //Delete column
        String colId = columns.get(0).getId()._getId();
        String tableId = tableInstance.getId()._getId();

        init();

        EntityMutationResponse deletionResponse = entityStore.deleteById(colId);
        assertEquals(deletionResponse.getDeletedEntities().size(), 1);
        assertEquals(deletionResponse.getDeletedEntities().get(0).getGuid(), colId);
        assertEquals(deletionResponse.getUpdatedEntities().size(), 1);
        assertEquals(deletionResponse.getUpdatedEntities().get(0).getGuid(), tableId);
        assertEntityDeleted(colId);

        tableInstance = metadataService.getEntityDefinitionReference(TestUtils.TABLE_TYPE, NAME, (String) tableEntity.getAttribute(NAME));
        assertDeletedColumn(tableInstance);

        //update by removing a column - col1
        tableEntity.setAttribute(COLUMNS_ATTR_NAME, Arrays.asList(column3Created.getAtlasObjectId()));

        init();
        deletionResponse = entityStore.createOrUpdate(tableEntityMap);

        assertEquals(deletionResponse.getDeletedEntities().size(), 1);
        assertEquals(deletionResponse.getDeletedEntities().get(0).getGuid(), column2Created.getGuid());
        assertEntityDeleted(colId);

        // Delete the table entities.  The deletion should cascade to their composite columns.
        tableInstance = metadataService.getEntityDefinitionReference(TestUtils.TABLE_TYPE, NAME, (String) tableEntity.getAttribute(NAME));

        init();
        EntityMutationResponse tblDeletionResponse = entityStore.deleteById(tableInstance.getId()._getId());
        assertEquals(tblDeletionResponse.getDeletedEntities().size(), 2);

        final AtlasEntityHeader tableDeleted = tblDeletionResponse.getFirstDeletedEntityByTypeName(TABLE_TYPE);
        final AtlasEntityHeader colDeleted = tblDeletionResponse.getFirstDeletedEntityByTypeName(COLUMN_TYPE);

        // Verify that deleteEntities() response has guids for tables and their composite columns.
        Assert.assertTrue(tableDeleted.getGuid().equals(tableInstance.getId()._getId()));
        Assert.assertTrue(colDeleted.getGuid().equals(column3Created.getGuid()));

        // Verify that tables and their composite columns have been deleted from the graph Repository.
        assertEntityDeleted(tableDeleted.getGuid());
        assertEntityDeleted(colDeleted.getGuid());
        assertTestDeleteEntities(tableInstance);
    }

    protected abstract void assertDeletedColumn(ITypedReferenceableInstance tableInstance) throws AtlasException;

    protected abstract void assertTestDeleteEntities(ITypedReferenceableInstance tableInstance) throws Exception;

    protected abstract void assertTableForTestDeleteReference(String tableId) throws Exception;

    protected abstract void assertColumnForTestDeleteReference(AtlasEntity tableInstance)
        throws AtlasException;

    protected abstract void assertProcessForTestDeleteReference(AtlasEntityHeader processInstance) throws Exception;

    protected abstract void assertEntityDeleted(String id) throws Exception;

    String getFirstGuid(Map<String, AtlasEntity> entityMap) {
        return entityMap.keySet().iterator().next();
    }

    @Test
    public void testUpdateEntity_MultiplicityOneNonCompositeReference() throws Exception {
        Map<String, AtlasEntity> hrDept = TestUtilsV2.createDeptEg1();
        final EntityMutationResponse hrDeptCreationResponse = entityStore.createOrUpdate(hrDept);

        ITypedReferenceableInstance hrDeptInstance = metadataService.getEntityDefinition(hrDeptCreationResponse.getFirstCreatedEntityByTypeName(TestUtilsV2.DEPARTMENT_TYPE).getGuid());
        Map<String, String> nameGuidMap = getEmployeeNameGuidMap(hrDeptInstance);

        ITypedReferenceableInstance john = metadataService.getEntityDefinition(nameGuidMap.get("John"));
        Id johnGuid = john.getId();

        ITypedReferenceableInstance max = metadataService.getEntityDefinition(nameGuidMap.get("Max"));
        String maxGuid = max.getId()._getId();
        AtlasVertex vertex = GraphHelper.getInstance().getVertexForGUID(maxGuid);
        Long creationTimestamp = GraphHelper.getSingleValuedProperty(vertex, Constants.TIMESTAMP_PROPERTY_KEY, Long.class);
        Assert.assertNotNull(creationTimestamp);

        Long modificationTimestampPreUpdate = GraphHelper.getSingleValuedProperty(vertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);
        Assert.assertNotNull(modificationTimestampPreUpdate);

        ITypedReferenceableInstance jane = metadataService.getEntityDefinition(nameGuidMap.get("Jane"));
        Id janeGuid = jane.getId();

        // Update max's mentor reference to john.
        ClassType personType = typeSystem.getDataType(ClassType.class, "Person");
        ITypedReferenceableInstance maxEntity = personType.createInstance(max.getId());
        maxEntity.set("mentor", johnGuid);
        AtlasClient.EntityResult entityResult = updatePartial(maxEntity);
        assertEquals(entityResult.getUpdateEntities().size(), 1);
        assertTrue(entityResult.getUpdateEntities().contains(maxGuid));

        // Verify the update was applied correctly - john should now be max's mentor.
        max = repositoryService.getEntityDefinition(maxGuid);
        ITypedReferenceableInstance refTarget = (ITypedReferenceableInstance) max.get("mentor");
        Assert.assertEquals(refTarget.getId()._getId(), johnGuid._getId());

        // Verify modification timestamp was updated.
        vertex = GraphHelper.getInstance().getVertexForGUID(maxGuid);
        Long modificationTimestampPostUpdate = GraphHelper.getSingleValuedProperty(vertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);
        Assert.assertNotNull(modificationTimestampPostUpdate);
        Assert.assertTrue(creationTimestamp < modificationTimestampPostUpdate);

        // Update max's mentor reference to jane.
        maxEntity.set("mentor", janeGuid);
        entityResult = updatePartial(maxEntity);
        assertEquals(entityResult.getUpdateEntities().size(), 1);
        assertTrue(entityResult.getUpdateEntities().contains(maxGuid));

        // Verify the update was applied correctly - jane should now be max's mentor.
        max = repositoryService.getEntityDefinition(maxGuid);
        refTarget = (ITypedReferenceableInstance) max.get("mentor");
        Assert.assertEquals(refTarget.getId()._getId(), janeGuid._getId());

        // Verify modification timestamp was updated.
        vertex = GraphHelper.getInstance().getVertexForGUID(maxGuid);
        Long modificationTimestampPost2ndUpdate = GraphHelper.getSingleValuedProperty(vertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);
        Assert.assertNotNull(modificationTimestampPost2ndUpdate);
        Assert.assertTrue(modificationTimestampPostUpdate < modificationTimestampPost2ndUpdate);

        ITypedReferenceableInstance julius = repositoryService.getEntityDefinition(nameGuidMap.get("Julius"));
        Id juliusGuid = julius.getId();
        maxEntity = personType.createInstance(max.getId());
        maxEntity.set("manager", juliusGuid);
        entityResult = updatePartial(maxEntity);
        //TODO ATLAS-499 should have updated julius' subordinates
        assertEquals(entityResult.getUpdateEntities().size(), 2);
        assertTrue(entityResult.getUpdateEntities().contains(maxGuid));
        assertTrue(entityResult.getUpdateEntities().contains(janeGuid._getId()));

        // Verify the update was applied correctly - julius should now be max's manager.
        max = repositoryService.getEntityDefinition(maxGuid);
        refTarget = (ITypedReferenceableInstance) max.get("manager");
        Assert.assertEquals(refTarget.getId()._getId(), juliusGuid._getId());

        assertTestUpdateEntity_MultiplicityOneNonCompositeReference(janeGuid._getId());
    }

    private Map<String, String> getEmployeeNameGuidMap(final ITypedReferenceableInstance hrDept) throws AtlasException {
        Object refValue = hrDept.get("employees");
        Assert.assertTrue(refValue instanceof List);
        List<Object> employees = (List<Object>)refValue;
        Assert.assertEquals(employees.size(), 4);
        Map<String, String> nameGuidMap = new HashMap<String, String>() {{
            put("hr", hrDept.getId()._getId());
        }};

        for (Object listValue : employees) {
            Assert.assertTrue(listValue instanceof ITypedReferenceableInstance);
            ITypedReferenceableInstance employee = (ITypedReferenceableInstance) listValue;
            nameGuidMap.put((String)employee.get("name"), employee.getId()._getId());
        }
        return nameGuidMap;
    }

    protected abstract void assertTestUpdateEntity_MultiplicityOneNonCompositeReference(String janeGuid) throws Exception;
}
