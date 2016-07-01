package org.apache.atlas.catalog;

import com.google.inject.Inject;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.catalog.definition.TermResourceDefinition;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.exception.TypeNotFoundException;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Guice(modules = RepositoryMetadataModule.class)
public class DefaultTypeSystemTest {

    @Inject
    private MetadataService metadataService;

    @Inject
    private GraphProvider<TitanGraph> graphProvider;

    private AtlasTypeSystem catalogTypeService;

    private static final String TEST_TAXONOMY = "testTaxonomy-" + RandomStringUtils.randomAlphanumeric(10);

    private Referenceable testDBEntity;
    private String testDBEntityId;

    @BeforeTest
    public void setUp() throws Exception {
        catalogTypeService = new DefaultTypeSystem(metadataService);

        TypesDef typesDef = TestUtils.defineHiveTypes();
        try {
            metadataService.getTypeDefinition(TestUtils.TABLE_TYPE);
        } catch (TypeNotFoundException e) {
            metadataService.createType(TypesSerialization.toJson(typesDef));
        }

        testDBEntity = TestUtils.createDBEntity();
        testDBEntityId = TestUtils.createInstance(metadataService, testDBEntity);
    }

    @AfterTest
    public void shutdown() throws Exception {
        TypeSystem.getInstance().reset();
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

    @Test
    public void testTraitCreation() throws Exception {
        catalogTypeService.createTraitType(new TermResourceDefinition(), TEST_TAXONOMY, TEST_TAXONOMY + "-description");
        String typesJson = metadataService.getTypeDefinition(TEST_TAXONOMY);
        final TypesDef typesDef = TypesSerialization.fromJson(typesJson);
        final List<HierarchicalTypeDefinition<TraitType>> hierarchicalTypeDefinitions = typesDef.traitTypesAsJavaList();
        Assert.assertEquals(hierarchicalTypeDefinitions.size(), 1);
        HierarchicalTypeDefinition<TraitType> traitType = hierarchicalTypeDefinitions.get(0);
        Assert.assertTrue(traitType.superTypes.contains(AtlasConstants.TAXONOMY_TERM_TYPE));

        Map<String, Object> expectedRequestProps = new HashMap<>();
        expectedRequestProps.put("name", "testTaxonomy.termName");
        // when not specified, the default value of 'true' should be set
        expectedRequestProps.put("available_as_tag", true);
        catalogTypeService.createTraitInstance(testDBEntityId, TEST_TAXONOMY, expectedRequestProps);

        final String traitDefinitionJson = metadataService.getTraitDefinition(testDBEntityId, TEST_TAXONOMY);
        final Struct traitDef = InstanceSerialization.fromJsonStruct(traitDefinitionJson, true);
        String nameSpace = (String) traitDef.get(AtlasConstants.NAMESPACE_ATTRIBUTE_NAME);
        Assert.assertEquals(nameSpace, AtlasConstants.TAXONOMY_NS);
    }
}
