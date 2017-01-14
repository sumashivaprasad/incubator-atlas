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
package org.apache.atlas.repository.store.graph.v1;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.IInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.ReferenceableInstance;
import org.apache.atlas.typesystem.persistence.StructInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Guice(modules = RepositoryMetadataModule.class)
public class AtlasEntityStoreV1Test {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityStoreV1Test.class);

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    AtlasTypeDefStore typeDefStore;

    @Inject
    AtlasEntityStore entityStore;

    @Inject
    MetadataService metadataService;
    
    private AtlasEntity entityCreated;

    @BeforeClass
    public void setUp() throws Exception {
        new GraphBackedSearchIndexer(typeRegistry);
        final AtlasTypesDef atlasTypesDef = TestUtilsV2.defineDeptEmployeeTypes();
        typeDefStore.createTypesDef(atlasTypesDef);
        
        entityCreated = TestUtilsV2.createDeptEg1();
    }

    @AfterClass
    public void clear(){
        AtlasGraphProvider.cleanup();
    }

    @Test
    public void testCreate() throws Exception {
        EntityMutationResponse response = entityStore.createOrUpdate(entityCreated);
        List<AtlasEntityHeader> entitiesCreated = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE);
        Assert.assertNotNull(entitiesCreated);
        Assert.assertEquals(entitiesCreated.size(), 5);

        AtlasEntityHeader deptEntity = entitiesCreated.get(0);

        //TODO : Use the older API for get until new instance API is ready.
        ITypedReferenceableInstance instance = metadataService.getEntityDefinition(deptEntity.getGuid());
        assertAttributes(deptEntity, instance);

    }

    private void assertAttributes(AtlasStruct entity, IInstance instance) throws AtlasBaseException, AtlasException {
        LOG.debug("Asserting type : " + entity.getTypeName());
        AtlasStructType entityType = (AtlasStructType) typeRegistry.getType(instance.getTypeName());
        for (String attrName : entity.getAttributes().keySet()) {
            Object actual = entity.getAttribute(attrName);
            Object expected = instance.get(attrName);

            AtlasType attrType = entityType.getAttributeType(attrName);
            assertAttribute(actual, expected, attrType, attrName);
        }
    }

    private void assertAttribute(Object actual, Object expected, AtlasType attributeType, String attrName) throws AtlasBaseException, AtlasException {
        LOG.debug("Asserting attribute : " + attrName);

        switch(attributeType.getTypeCategory()) {
        case ENTITY:
            if ( expected instanceof  Id) {
                String guid = ((Id) expected)._getId();
                Assert.assertTrue(AtlasEntity.isAssigned(guid));
            } else {
                ReferenceableInstance expectedInstance = (ReferenceableInstance) expected;
                AtlasEntity actualInstance = (AtlasEntity) actual;
                assertAttributes(actualInstance, expectedInstance);
            }
            break;
        case PRIMITIVE:
        case ENUM:
            Assert.assertEquals(actual, expected);
            break;
        case MAP:
            AtlasMapType mapType = (AtlasMapType) attributeType;
            AtlasType keyType = mapType.getKeyType();
            AtlasType valueType = mapType.getValueType();
            Map actualMap = (Map) actual;
            Map expectedMap = (Map) expected;

            Assert.assertEquals(actualMap.size(), expectedMap.size());
            for (Object key : actualMap.keySet()) {
                assertAttribute(actualMap.get(key), expectedMap.get(key), valueType, attrName);
            }
            break;
        case ARRAY:
            AtlasArrayType arrType = (AtlasArrayType) attributeType;
            AtlasType elemType = arrType.getElementType();
            List actualList = (List) actual;
            List expectedList = (List) expected;

            if (!(expected == null && actualList.size() == 0)) {
                Assert.assertEquals(actualList.size(), expectedList.size());
                for (int i = 0; i < actualList.size(); i++) {
                    assertAttribute(actualList.get(i), expectedList.get(i), elemType, attrName);
                }
            }
            break;
        case STRUCT:
            StructInstance structInstance = (StructInstance) expected;
            AtlasStruct newStructVal = (AtlasStruct) actual;
            assertAttributes(newStructVal, structInstance);
            break;
        default:
            Assert.fail("Unknown type category");
        }
    }

    @Test(dependsOnMethods = "testCreate")
    public void testArrayUpdate() throws Exception {
        AtlasEntity entityClone = new AtlasEntity(entityCreated);

        List<AtlasEntity> employees = (List<AtlasEntity>) entityClone.getAttribute("employees");

        List<AtlasEntity> updatedEmployees = new ArrayList<>(employees);
        clearSubOrdinates(updatedEmployees);
        entityClone.setAttribute("employees", updatedEmployees);

        EntityMutationResponse response = entityStore.createOrUpdate(entityClone);
        
        List<AtlasEntityHeader> entitiesUpdated = response.getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE);
        Assert.assertNotNull(entitiesUpdated);
        Assert.assertEquals(entitiesUpdated.size(), 5);

        AtlasEntityHeader deptEntity = entitiesUpdated.get(0);

        //TODO : Change to new API after new instance GET API is ready.
        ITypedReferenceableInstance instance = metadataService.getEntityDefinition(deptEntity.getGuid());
        assertAttributes(deptEntity, instance);

    }

    private void clearSubOrdinates(List<AtlasEntity> updatedEmployees) {
        List<AtlasEntity> subOrdinates = (List<AtlasEntity>) updatedEmployees.get(1).getAttribute("subordinates");
        List<AtlasEntity> subOrdClone = new ArrayList<>(subOrdinates);
        subOrdClone.remove(1);

        updatedEmployees.get(1).setAttribute("subordinates", subOrdClone);
    }
}
