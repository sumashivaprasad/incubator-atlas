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

import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.Referenceable;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.List;

@Guice(modules = {AtlasFormatConvertersModule.class, RepositoryMetadataModule.class})
public class AtlasEntityToEntityConverterTest {

    @Inject
    private AtlasTypeDefStore typeStore;

    @Inject
    private AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasEntityToReferenceableConverter converter;

    @BeforeClass
    public void setUp() throws Exception {
        AtlasTypesDef typesDef = TestUtilsV2.defineHiveTypes();
        typeStore.createTypesDef(typesDef);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        AtlasGraphProvider.cleanup();
    }

    @Test
    public void testConvert() throws Exception {
        final AtlasEntity dbEntity = TestUtilsV2.createDBEntity();
        final AtlasEntity tableEntity = TestUtilsV2.createTableEntity(dbEntity.getGuid());

        String typeName = tableEntity.getTypeName();

        AtlasEntityDef tblDef = typeRegistry.getEntityDefByName(typeName);
        Referenceable ref = converter.convert(tableEntity);

        for ( AtlasStructDef.AtlasAttributeDef attrDef: tblDef.getAttributeDefs()) {
            String attrTypeName = attrDef.getTypeName();
            AtlasType attrType = typeRegistry.getType(attrTypeName);
            if (isPrimitiveOrCollectionWithPrimitives(attrType)) {
                Assert.assertEquals(ref.get(attrDef.getName()), tableEntity.getAttribute(attrDef.getName()));
            } else if (isArrayType(attrType)) {
//                compareLists(attrType, tableEntity, )
            }
            //TODO - Compare arrays and maps

            //TODO - Compare entity and referenceable
        }
    }

    private boolean isPrimitiveOrCollectionWithPrimitives(AtlasType attrType) {
        if ( attrType.getTypeCategory() == AtlasType.TypeCategory.PRIMITIVE ||
            ((attrType.getTypeCategory() == AtlasType.TypeCategory.ARRAY || attrType.getTypeCategory() == AtlasType.TypeCategory.MAP) &&
                ((AtlasArrayType) attrType).getElementType().getTypeCategory() == AtlasType.TypeCategory.PRIMITIVE)) {
            return true;
        }
        return false;
    }

    private boolean isArrayType(AtlasType atlasType) {
        if (AtlasArrayType.class.equals(atlasType.getClass())) {
            return true;
        }
        return false;
    }

//    boolean compareLists(AtlasArrayType type, List<? extends AtlasStruct> list1, List<? extends Struct> list2) {
//        for (AtlasStruct l : list1) {
//            l.equals()
//        }
//    }
}
