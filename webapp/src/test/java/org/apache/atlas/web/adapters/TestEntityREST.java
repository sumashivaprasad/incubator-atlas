package org.apache.atlas.web.adapters;

import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.web.rest.EntityREST;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

@Guice(modules = {AtlasFormatConvertersModule.class, RepositoryMetadataModule.class})
public class TestEntityREST {

    @Inject
    private AtlasTypeDefStore typeStore;

    @Inject
    private EntityREST entityREST;

    private AtlasEntity dbEntity;

    private String dbGuid;

    @BeforeClass
    public void setUp() throws Exception {
        AtlasTypesDef typesDef = TestUtilsV2.defineHiveTypes();
        typeStore.createTypesDef(typesDef);
        dbEntity = TestUtilsV2.createDBEntity();
    }

    @Test
    public void testCreateOrUpdateEntity() throws Exception {
        final EntityMutationResponse response = entityREST.createOrUpdate(dbEntity);

        Assert.assertNotNull(response);
        List<AtlasEntityHeader> entitiesMutated = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE_OR_UPDATE);
        dbGuid = entitiesMutated.get(0).getGuid();
        Assert.assertEquals(entitiesMutated.size(), 1);
    }

    @Test(dependsOnMethods = "testCreateOrUpdateEntity")
    public void testGetEntity() throws Exception {

        final AtlasEntity response = entityREST.getById(dbGuid);

        Assert.assertNotNull(response);
        TestAtlasEntitiesREST.verifyAttributes(response.getAttributes(), dbEntity.getAttributes());
    }
}
