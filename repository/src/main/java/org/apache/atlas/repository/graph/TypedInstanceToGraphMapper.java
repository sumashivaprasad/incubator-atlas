package org.apache.atlas.repository.graph;


import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.SchemaViolationException;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.EntityExistsException;
import org.apache.atlas.repository.EntityNotFoundException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.ITypedInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.ReferenceableInstance;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.ObjectGraphWalker;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TypedInstanceToGraphMapper {

    private static final String FULL_TEXT_DELIMITER = " ";
    private static final Logger LOG = LoggerFactory.getLogger(TypedInstanceToGraphMapper.class);
    BiMap<Vertex, ITypedReferenceableInstance> vertexToInstanceMap = HashBiMap.create();
    private final TypeSystem typeSystem = TypeSystem.getInstance();
    private final TitanGraph titanGraph;
    private final GraphToTypedInstanceMapper graphToTypedInstanceMapper;

    public TypedInstanceToGraphMapper(TitanGraph titanGraph, GraphToTypedInstanceMapper graphToTypedInstanceMapper) throws AtlasException {
        this.titanGraph = titanGraph;
        this.graphToTypedInstanceMapper = graphToTypedInstanceMapper;
    }

    private final class EntityProcessor implements ObjectGraphWalker.NodeProcessor {

        public final Map<Id, IReferenceableInstance> idToInstanceMap;
        public final Map<Id, Vertex> idToVertexMap;

        public EntityProcessor() {
            idToInstanceMap = new LinkedHashMap<>();
            idToVertexMap = new HashMap<>();
        }

        public void cleanUp() {
            idToInstanceMap.clear();
        }

        @Override
        public void processNode(ObjectGraphWalker.Node nd) throws AtlasException {
            IReferenceableInstance ref = null;
            Id id = null;

            if (nd.attributeName == null) {
                ref = (IReferenceableInstance) nd.instance;
                id = ref.getId();
            } else if (nd.aInfo.dataType().getTypeCategory() == DataTypes.TypeCategory.CLASS) {
                if (nd.value != null && (nd.value instanceof Id)) {
                    id = (Id) nd.value;
                }
            }

            if (id != null) {
                if (id.isUnassigned()) {
                    if (ref != null) {
                        if (idToInstanceMap.containsKey(id)) { // Oops
                            throw new RepositoryException(
                                String.format("Unexpected internal error: Id %s processed again", id));
                        }

                        idToInstanceMap.put(id, ref);
                    }
                } else {
                    //Updating entity
                    if (ref != null) {
                        idToInstanceMap.put(id, ref);
                    }
                }
            }
        }

//        private List<ITypedReferenceableInstance> updateOrCreateVerticesForClassType (
//            List<ITypedReferenceableInstance> typedInstances) throws AtlasException {
//
//            List<ITypedReferenceableInstance> instances = new ArrayList<>();
//            for (ITypedReferenceableInstance typedInstance : typedInstances) {
//                final Id id = typedInstance.getId();
//                if (!idToVertexMap.containsKey(id)) {
//                    Vertex instanceVertex;
//                    ClassType classType = typeSystem.getDataType(ClassType.class, typedInstance.getTypeName());
//                    if (id.isAssigned()) {  // has a GUID
//                        instanceVertex = GraphHelper.getVertexForGUID(titanGraph, id.id);
//                    } else {
//                        //Check if there is already an instance with the same unique attribute value
//                        instanceVertex = getVertexForInstanceByUniqueAttribute(classType, typedInstance);
//                        if (instanceVertex == null) {
//                            instanceVertex = GraphHelper.createVertexWithIdentity(titanGraph, typedInstance,
//                                classType.getAllSuperTypeNames());
//                            Id newId = new Id((String)instanceVertex.getProperty(Constants.GUID_PROPERTY_KEY), id.version, id.className);
//                            ((ReferenceableInstance) typedInstance).replaceWithNewId(newId);
//
//                            mapInstanceToVertex(id, typedInstance, instanceVertex,
//                                classType.fieldMapping().fields, idToVertexMap, true);
//                        }
//                    }
//
//                    instances.add(typedInstance);
//                    idToVertexMap.put(id, instanceVertex);
//                    vertexToInstanceMap.put(instanceVertex, typedInstance);
//                }
//            }
//            return instances;
//        }

        private ITypedReferenceableInstance updateByUniqueAttribute(
            String uniqeAttrName, String attrValue, ITypedReferenceableInstance typedInstance) throws AtlasException {

            final Id id = typedInstance.getId();
            if (!idToVertexMap.containsKey(id)) {
                Vertex instanceVertex;
                ClassType classType = typeSystem.getDataType(ClassType.class, typedInstance.getTypeName());
                if (id.isAssigned()) {  // has a GUID
                    instanceVertex = GraphHelper.getVertexForGUID(titanGraph, id.id);
                } else {
                    //Check if there is already an instance with the given unique attribute value
                    instanceVertex = GraphHelper.getVertexForProperty(titanGraph, uniqeAttrName, attrValue);
                    if (instanceVertex == null) {
                        throw new EntityNotFoundException(String.format("Entity with unique attribute(%s, %s) not found", uniqeAttrName, attrValue));
                    }
                    mapInstanceToVertex(id, typedInstance, instanceVertex,
                                classType.fieldMapping().fields, idToVertexMap, true);

                }
                idToVertexMap.put(id, instanceVertex);
                vertexToInstanceMap.put(instanceVertex, typedInstance);
            }
            return typedInstance;
        }

        private List<ITypedReferenceableInstance> createVerticesForClassType(
            List<ITypedReferenceableInstance> typedInstances) throws AtlasException {

            List<ITypedReferenceableInstance> instancesCreated = new ArrayList<>();
            for (ITypedReferenceableInstance typedInstance : typedInstances) {
                final Id id = typedInstance.getId();
                if (!idToVertexMap.containsKey(id)) {
                    Vertex instanceVertex;
                    if (id.isAssigned()) {  // has a GUID
                        instanceVertex = GraphHelper.getVertexForGUID(titanGraph, id.id);
                    } else {
                        //Check if there is already an instance with the same unique attribute value
                        ClassType classType = typeSystem.getDataType(ClassType.class, typedInstance.getTypeName());
                        instanceVertex = getVertexForInstanceByUniqueAttribute(classType, typedInstance);
                        if (instanceVertex == null) {
                            instanceVertex = GraphHelper.createVertexWithIdentity(titanGraph, typedInstance,
                                classType.getAllSuperTypeNames());
                            instancesCreated.add(typedInstance);

                            mapInstanceToVertex(id, typedInstance, instanceVertex,
                                classType.fieldMapping().fields, idToVertexMap, true);

                            Id newId = new Id((String)instanceVertex.getProperty(Constants.GUID_PROPERTY_KEY), id.version, id.className);
                            ((ReferenceableInstance) typedInstance).replaceWithNewId(newId);
                        }
                    }

                    idToVertexMap.put(id, instanceVertex);
                    vertexToInstanceMap.put(instanceVertex, typedInstance);
                }
            }
            return instancesCreated;
        }
    }

    String[] mapTypedInstanceToGraph(ITypedReferenceableInstance... typedInstances)
        throws AtlasException {
        EntityProcessor entityProcessor = new EntityProcessor();
        List<String> guids = new ArrayList<>();
        for (ITypedReferenceableInstance typedInstance : typedInstances) {
            List<ITypedReferenceableInstance> newTypedInstances = walkAndDiscoverClassInstances(entityProcessor, typedInstance);
            List<ITypedReferenceableInstance> instancesCreated =
                entityProcessor.createVerticesForClassType(newTypedInstances);

            addAttributesAndTraits(entityProcessor, instancesCreated);

            addFullTextProperty(entityProcessor);

            //Return guid for
            addToGuids(typedInstance, guids);
        }
        return guids.toArray(new String[guids.size()]);
    }

    private List<ITypedReferenceableInstance> walkAndDiscoverClassInstances(EntityProcessor entityProcessor, ITypedReferenceableInstance typedInstance) throws RepositoryException {
        try {
            LOG.debug("Walking the object graph for instance {}", typedInstance.getTypeName());
            entityProcessor.cleanUp();
            new ObjectGraphWalker(typeSystem, entityProcessor, typedInstance).walk();
        } catch (AtlasException me) {
            throw new RepositoryException("TypeSystem error when walking the ObjectGraph", me);
        }

        return discoverInstances(entityProcessor);
    }

    private void addAttributesAndTraits(EntityProcessor entityProcessor, List<ITypedReferenceableInstance> instancesCreated) throws AtlasException {
        for (ITypedReferenceableInstance instance : instancesCreated) {
            try {
                //new vertex, set all the properties
                addDiscoveredInstance(entityProcessor, instance);
            } catch (SchemaViolationException e) {
                throw new EntityExistsException(instance, e);
            }
        }
    }

    String updateGraphByUniqueAttribute(String uniqueAttrName, String uniqAttrValue, ITypedReferenceableInstance typedInstance)
        throws AtlasException {
        EntityProcessor entityProcessor = new EntityProcessor();
        ITypedReferenceableInstance instanceUpdated =
            entityProcessor.updateByUniqueAttribute(uniqueAttrName, uniqAttrValue, typedInstance);

        List<ITypedReferenceableInstance> newTypedInstances = walkAndDiscoverClassInstances(entityProcessor, instanceUpdated);
        List<ITypedReferenceableInstance> instancesCreated =
            entityProcessor.createVerticesForClassType(newTypedInstances);



        addAttributesAndTraits(entityProcessor, instancesCreated);

        addFullTextProperty(entityProcessor);

        //Return guid for
        Vertex instanceVertex = vertexToInstanceMap.inverse().get(typedInstance);
        return instanceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
    }

    private void addToGuids(ITypedReferenceableInstance typedInstance, List<String> guids) {
        Vertex instanceVertex = vertexToInstanceMap.inverse().get(typedInstance);
        String guid = instanceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
        guids.add(guid);

//        Vertex instanceVertex = entityProcessor.idToVertexMap.get(typedInstance.getId());
//        String guid = instanceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
//        guids.add(guid);
    }

    private void addFullTextProperty(EntityProcessor entityProcessor) throws AtlasException {
        for (ITypedReferenceableInstance typedInstance : vertexToInstanceMap.values()) { // Traverse
            Vertex instanceVertex = entityProcessor.idToVertexMap.get(typedInstance.getId());
            String fullText = getFullTextForVertex(instanceVertex, true);
            GraphHelper.setProperty(instanceVertex, Constants.ENTITY_TEXT_PROPERTY_KEY, fullText);
        }
    }

    private String getFullTextForVertex(Vertex instanceVertex, boolean followReferences) throws AtlasException {
        String guid = instanceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
        ITypedReferenceableInstance typedReference =
            graphToTypedInstanceMapper.mapGraphToTypedInstance(guid, instanceVertex);
        String fullText = getFullTextForInstance(typedReference, followReferences);
        StringBuilder fullTextBuilder =
            new StringBuilder(typedReference.getTypeName()).append(FULL_TEXT_DELIMITER).append(fullText);

        List<String> traits = typedReference.getTraits();
        for (String traitName : traits) {
            String traitText = getFullTextForInstance((ITypedInstance) typedReference.getTrait(traitName), false);
            fullTextBuilder.append(FULL_TEXT_DELIMITER).append(traitName).append(FULL_TEXT_DELIMITER)
                .append(traitText);
        }
        return fullTextBuilder.toString();
    }

    private String getFullTextForAttribute(IDataType type, Object value, boolean followReferences)
        throws AtlasException {
        if (value == null) {
            return null;
        }
        switch (type.getTypeCategory()) {
        case PRIMITIVE:
            return String.valueOf(value);
        case ENUM:

            return ((EnumValue) value).value;

        case ARRAY:
            StringBuilder fullText = new StringBuilder();
            IDataType elemType = ((DataTypes.ArrayType) type).getElemType();
            List list = (List) value;

            for (Object element : list) {
                String elemFullText = getFullTextForAttribute(elemType, element, false);
                if (StringUtils.isNotEmpty(elemFullText)) {
                    fullText = fullText.append(FULL_TEXT_DELIMITER).append(elemFullText);
                }
            }
            return fullText.toString();

        case MAP:
            fullText = new StringBuilder();
            IDataType keyType = ((DataTypes.MapType) type).getKeyType();
            IDataType valueType = ((DataTypes.MapType) type).getValueType();
            Map map = (Map) value;

            for (Object entryObj : map.entrySet()) {
                Map.Entry entry = (Map.Entry) entryObj;
                String keyFullText = getFullTextForAttribute(keyType, entry.getKey(), false);
                if (StringUtils.isNotEmpty(keyFullText)) {
                    fullText = fullText.append(FULL_TEXT_DELIMITER).append(keyFullText);
                }
                String valueFullText = getFullTextForAttribute(valueType, entry.getValue(), false);
                if (StringUtils.isNotEmpty(valueFullText)) {
                    fullText = fullText.append(FULL_TEXT_DELIMITER).append(valueFullText);
                }
            }
            return fullText.toString();

        case CLASS:
            if (followReferences) {
                String refGuid = ((ITypedReferenceableInstance) value).getId()._getId();
                Vertex refVertex = GraphHelper.getVertexForGUID(titanGraph, refGuid);
                return getFullTextForVertex(refVertex, false);
            }
            break;

        case STRUCT:
            if (followReferences) {
                return getFullTextForInstance((ITypedInstance) value, false);
            }
            break;

        default:
            throw new IllegalStateException("Unhandled type category " + type.getTypeCategory());

        }
        return null;
    }

    private String getFullTextForInstance(ITypedInstance typedInstance, boolean followReferences)
        throws AtlasException {
        StringBuilder fullText = new StringBuilder();
        for (AttributeInfo attributeInfo : typedInstance.fieldMapping().fields.values()) {
            Object attrValue = typedInstance.get(attributeInfo.name);
            if (attrValue == null) {
                continue;
            }

            String attrFullText = getFullTextForAttribute(attributeInfo.dataType(), attrValue, followReferences);
            if (StringUtils.isNotEmpty(attrFullText)) {
                fullText =
                    fullText.append(FULL_TEXT_DELIMITER).append(attributeInfo.name).append(FULL_TEXT_DELIMITER)
                        .append(attrFullText);
            }
        }
        return fullText.toString();
    }

    /**
     * Step 2: Traverse oldIdToInstance map create newInstances :
     * List[ITypedReferenceableInstance]
     * - create a ITypedReferenceableInstance.
     * replace any old References ( ids or object references) with new Ids.
     */
    private List<ITypedReferenceableInstance> discoverInstances(EntityProcessor entityProcessor)
        throws RepositoryException {
        List<ITypedReferenceableInstance> newTypedInstances = new ArrayList<>();
        for (IReferenceableInstance transientInstance : entityProcessor.idToInstanceMap.values()) {
            LOG.debug("Discovered instance {}", transientInstance.getTypeName());
            try {
                ClassType cT = typeSystem.getDataType(ClassType.class, transientInstance.getTypeName());
                ITypedReferenceableInstance newInstance = cT.convert(transientInstance, Multiplicity.REQUIRED);
                newTypedInstances.add(newInstance);
            } catch (AtlasException me) {
                throw new RepositoryException(
                    String.format("Failed to create Instance(id = %s", transientInstance.getId()), me);
            }
        }

        //Reverse the list to create the entities in dependency order
        return Lists.reverse(newTypedInstances);
    }

    /**
     * For the given type, finds an unique attribute and checks if there is an existing instance with the same
     * unique value
     *
     * @param classType
     * @param instance
     * @return
     * @throws AtlasException
     */
    Vertex getVertexForInstanceByUniqueAttribute(ClassType classType, IReferenceableInstance instance)
        throws AtlasException {
        for (AttributeInfo attributeInfo : classType.fieldMapping().fields.values()) {
            if (attributeInfo.isUnique) {
                String propertyKey = GraphHelper.getQualifiedFieldName(classType, attributeInfo.name);
                try {
                    return GraphHelper.getVertexForProperty(titanGraph, propertyKey, instance.get(attributeInfo.name));
                } catch (EntityNotFoundException e) {
                    //Its ok if there is no entity with the same unique value
                }
            }
        }

        return null;
    }

    private void addDiscoveredInstance(EntityProcessor entityProcessor, ITypedReferenceableInstance typedInstance)
        throws AtlasException {
        LOG.debug("Adding typed instance {}", typedInstance.getTypeName());

        Id id = typedInstance.getId();
        if (id == null) { // oops
            throw new RepositoryException("id cannot be null");
        }

        Vertex instanceVertex = entityProcessor.idToVertexMap.get(id);

        // add the attributes for the instance
        ClassType classType = typeSystem.getDataType(ClassType.class, typedInstance.getTypeName());
        final Map<String, AttributeInfo> fields = classType.fieldMapping().fields;

        mapInstanceToVertex(id, typedInstance, instanceVertex, fields, entityProcessor.idToVertexMap, false);

        for (String traitName : typedInstance.getTraits()) {
            LOG.debug("mapping trait {}", traitName);
            GraphHelper.addProperty(instanceVertex, Constants.TRAIT_NAMES_PROPERTY_KEY, traitName);
            ITypedStruct traitInstance = (ITypedStruct) typedInstance.getTrait(traitName);

            // add the attributes for the trait instance
            mapTraitInstanceToVertex(traitInstance, typedInstance.getId(), classType, instanceVertex,
                entityProcessor.idToVertexMap);
        }
    }

    private void mapInstanceToVertex(Id id, ITypedInstance typedInstance, Vertex instanceVertex,
        Map<String, AttributeInfo> fields, Map<Id, Vertex> idToVertexMap, boolean mapOnlyUniqueAttributes)
        throws AtlasException {
        LOG.debug("Mapping instance {} of {} to vertex {}", typedInstance, typedInstance.getTypeName(),
            instanceVertex);
        for (AttributeInfo attributeInfo : fields.values()) {
            if (mapOnlyUniqueAttributes && !attributeInfo.isUnique) {
                continue;
            }
            final IDataType dataType = attributeInfo.dataType();
            mapAttributesToVertex(id, typedInstance, instanceVertex, idToVertexMap, attributeInfo, dataType);
        }
    }

    void mapAttributesToVertex(Id id, ITypedInstance typedInstance, Vertex instanceVertex,
        Map<Id, Vertex> idToVertexMap, AttributeInfo attributeInfo, IDataType dataType) throws AtlasException {
        Object attrValue = typedInstance.get(attributeInfo.name);
        LOG.debug("mapping attribute {} = {}", attributeInfo.name, attrValue);
        final String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        String edgeLabel = GraphHelper.getEdgeLabel(typedInstance, attributeInfo);
        if (attrValue == null) {
            return;
        }

        switch (dataType.getTypeCategory()) {
        case PRIMITIVE:
            mapPrimitiveToVertex(typedInstance, instanceVertex, attributeInfo);
            break;

        case ENUM:
            //handles both int and string for enum
            EnumValue enumValue =
                (EnumValue) dataType.convert(typedInstance.get(attributeInfo.name), Multiplicity.REQUIRED);

            Object oldPropertyValue = instanceVertex.getProperty(attributeInfo.name);
            if (oldPropertyValue == null || !oldPropertyValue.equals(enumValue.value)) {
                GraphHelper.setProperty(instanceVertex, propertyName, enumValue.value);
            }
            break;

        case ARRAY:
            mapArrayCollectionToVertex(id, typedInstance, instanceVertex, attributeInfo, idToVertexMap);
            break;

        case MAP:
            mapMapCollectionToVertex(id, typedInstance, instanceVertex, attributeInfo, idToVertexMap);
            break;

        case STRUCT:
            mapStructToVertex(id, (ITypedStruct) typedInstance.get(attributeInfo.name), instanceVertex, attributeInfo, idToVertexMap, edgeLabel);
            break;
        case TRAIT:
            // do NOTHING - this is taken care of earlier
            break;

        case CLASS:
            mapClassReferenceAsEdge(instanceVertex, idToVertexMap, edgeLabel, attributeInfo, dataType,
                (ITypedReferenceableInstance) attrValue);
            break;

        default:
            throw new IllegalArgumentException("Unknown type category: " + dataType.getTypeCategory());
        }
    }

    private Pair<Vertex, Edge> mapStructToVertex(Id id, ITypedStruct typedInstance, Vertex instanceVertex, AttributeInfo attributeInfo, Map<Id, Vertex> idToVertexMap, String edgeLabel) throws AtlasException {
        Vertex structInstanceVertex = null;
        Edge relEdge = null;
        final Iterable<Edge> edges = instanceVertex.getEdges(Direction.OUT, edgeLabel);
        Iterator<Edge> iter = edges.iterator();
        //Assuming that array to struct edges are already cleaned up by now and that struct mappsing are singletons
        if (edges != null && iter.hasNext()) {
            //Already existing vertex. Update
            relEdge = edges.iterator().next();
            structInstanceVertex = relEdge.getVertex(Direction.IN);

            if (typedInstance == null) {
                //Delete edge and remove struct vertex since struct attribute has been set to null
                removeUnusedReference(instanceVertex, relEdge.getId().toString(), attributeInfo, attributeInfo.dataType());
            } else {
                // Update attributes
                // map all the attributes to this  vertex
                mapInstanceToVertex(id, typedInstance, structInstanceVertex, typedInstance.fieldMapping().fields,
                    idToVertexMap, false);
            }
        } else {
            if (typedInstance == null) {
                return null;
            }
            // add a new vertex for the struct or trait instance
            structInstanceVertex = GraphHelper
                .createVertexWithoutIdentity(titanGraph, typedInstance.getTypeName(), id,
                    Collections.<String>emptySet()); // no super types for struct type
            LOG.debug("created vertex {} for struct {} value {}", structInstanceVertex, attributeInfo.name,
                typedInstance);

            // map all the attributes to this new vertex
            mapInstanceToVertex(id, typedInstance, structInstanceVertex, typedInstance.fieldMapping().fields,
                idToVertexMap, false);
            // add an edge to the newly created vertex from the parent
            relEdge = GraphHelper.addEdge(titanGraph, instanceVertex, structInstanceVertex, edgeLabel);
        }
        return Pair.of(structInstanceVertex, relEdge);
    }

    private void mapArrayCollectionToVertex(Id id, ITypedInstance typedInstance, Vertex instanceVertex,
        AttributeInfo attributeInfo, Map<Id, Vertex> idToVertexMap) throws AtlasException {
        LOG.debug("Mapping instance {} to vertex {} for name {}", typedInstance.getTypeName(), instanceVertex,
            attributeInfo.name);
        List list = (List) typedInstance.get(attributeInfo.name);
        if (list == null || list.isEmpty()) {
            return;
        }
        String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        final String edgeLabel = GraphHelper.EDGE_LABEL_PREFIX + propertyName;
        IDataType elementType = ((DataTypes.ArrayType) attributeInfo.dataType()).getElemType();

        List<String> currentEdgeIds = instanceVertex.getProperty(propertyName);
        if (currentEdgeIds != null) {
            for (String edgeId : currentEdgeIds) {
                removeUnusedReference(instanceVertex, edgeId, attributeInfo, elementType);
            }
        }

        List<String> values = new ArrayList<>();
        for (int index = 0; index < list.size(); index++) {
            String entryId =
                mapCollectionEntryToVertex(id, instanceVertex, attributeInfo, idToVertexMap, elementType,
                    list.get(index), edgeLabel + "_" + index);
            if (entryId != null) {
                values.add(entryId);
            }
        }

        // for dereference on way out
        GraphHelper.setProperty(instanceVertex, propertyName, values);
    }

    private Edge removeUnusedReference(Vertex instanceVertex, String edgeId, AttributeInfo attributeInfo, IDataType<?> elementType) {
        //Remove edges for property values which do not exist any more
        Edge removedRelation = null;
        switch (elementType.getTypeCategory()) {
        case STRUCT:
            removedRelation = GraphHelper.removeRelation(titanGraph, edgeId, true);
            //Remove the vertex from state so that further processing no longer uses this
            vertexToInstanceMap.remove(removedRelation.getVertex(Direction.IN));
            break;
        case CLASS:
            removedRelation = GraphHelper.removeRelation(titanGraph, edgeId, attributeInfo.isComposite);
            if (attributeInfo.isComposite) {
                //Remove the vertex from state so that further processing no longer uses this
                vertexToInstanceMap.remove(removedRelation.getVertex(Direction.IN));
            }
            break;
        }
        return removedRelation;
    }

    private void mapMapCollectionToVertex(Id id, ITypedInstance typedInstance, Vertex instanceVertex,
        AttributeInfo attributeInfo, Map<Id, Vertex> idToVertexMap) throws AtlasException {
        LOG.debug("Mapping instance {} to vertex {} for name {}", typedInstance.getTypeName(), instanceVertex,
            attributeInfo.name);
        @SuppressWarnings("unchecked") Map<Object, Object> collection =
            (Map<Object, Object>) typedInstance.get(attributeInfo.name);
        if (collection == null || collection.isEmpty()) {
            return;
        }

        String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        IDataType elementType = ((DataTypes.MapType) attributeInfo.dataType()).getValueType();

        for (Map.Entry entry : collection.entrySet()) {
            String myPropertyName = propertyName + "." + entry.getKey().toString();
            final String edgeLabel = GraphHelper.EDGE_LABEL_PREFIX + myPropertyName;
            String value = mapCollectionEntryToVertex(id, instanceVertex, attributeInfo, idToVertexMap, elementType,
                entry.getValue(), edgeLabel);

            //Add/Update/Remove property value
            GraphHelper.setProperty(instanceVertex, myPropertyName, value);
        }

        //Remove unused keys
        List<Object> origKeys = instanceVertex.getProperty(propertyName);
        if (origKeys != null) {
            origKeys.removeAll(collection.keySet());
            for (Object unusedKey : origKeys) {
                String edgeLabel = GraphHelper.EDGE_LABEL_PREFIX + propertyName + "." + unusedKey.toString();
                if (instanceVertex.getEdges(Direction.OUT, edgeLabel).iterator().hasNext()) {
                    Edge edge = instanceVertex.getEdges(Direction.OUT, edgeLabel).iterator().next();
                    removeUnusedReference(instanceVertex, edge.getId().toString(), attributeInfo, ((DataTypes.MapType) attributeInfo.dataType()).getValueType());
                }
            }
        }

        // for dereference on way out
        GraphHelper.setProperty(instanceVertex, propertyName, new ArrayList(collection.keySet()));
    }

    private String mapCollectionEntryToVertex(Id id, Vertex instanceVertex, AttributeInfo attributeInfo,
        Map<Id, Vertex> idToVertexMap, IDataType elementType, Object value, String edgeLabel)
        throws AtlasException {

        switch (elementType.getTypeCategory()) {
        case PRIMITIVE:
        case ENUM:
            return value != null ? value.toString() : null;

        case ARRAY:
        case MAP:
        case TRAIT:
            // do nothing
            return null;

        case STRUCT:
            ITypedStruct structAttr = (ITypedStruct) value;
            final Pair<Vertex, Edge> vertexEdgePair = mapStructToVertex(id, structAttr, instanceVertex, attributeInfo, idToVertexMap, edgeLabel);
            return (vertexEdgePair != null) ? vertexEdgePair.getRight().getId().toString() : null;

        case CLASS:
            return mapClassReferenceAsEdge(instanceVertex, idToVertexMap, edgeLabel,
                attributeInfo, elementType, (ITypedReferenceableInstance) value);

        default:
            throw new IllegalArgumentException("Unknown type category: " + elementType.getTypeCategory());
        }
    }

    private String mapClassReferenceAsEdge(Vertex instanceVertex, Map<Id, Vertex> idToVertexMap, String edgeLabel,
        AttributeInfo attributeInfo, IDataType<?> elementType, ITypedReferenceableInstance typedReference) throws AtlasException {

        final Iterable<Edge> edges = instanceVertex.getEdges(Direction.OUT, edgeLabel);
        Edge edge = null;
        if (edges != null && edges.iterator().hasNext()) {
            //Edge already exists. Assuming array to class edges are singletons. So the edge labels should have the index with them to keep them unique
            edge = edges.iterator().next();
        }

        if (edge == null && typedReference != null) {
            Vertex referenceVertex;
            Id id = typedReference instanceof Id ? (Id) typedReference : typedReference.getId();
            if (id.isAssigned()) {
                referenceVertex = GraphHelper.getVertexForGUID(titanGraph, id.id);
            } else {
                referenceVertex = idToVertexMap.get(id);
            }

            if (referenceVertex != null) {
                // add an edge to the class vertex from the instance
                edge = GraphHelper.addEdge(titanGraph, instanceVertex, referenceVertex, edgeLabel);
                return String.valueOf(edge.getId());
            }
        } else {
            //TODO . Update the Id
            // Do nothing since edge already exists
            return edge.getId().toString();
        }

        return null;
    }

    void mapTraitInstanceToVertex(ITypedStruct traitInstance, Id typedInstanceId,
        IDataType entityType, Vertex parentInstanceVertex, Map<Id, Vertex> idToVertexMap)
        throws AtlasException {
        // add a new vertex for the struct or trait instance
        final String traitName = traitInstance.getTypeName();
        Vertex traitInstanceVertex = GraphHelper
            .createVertexWithoutIdentity(titanGraph, traitInstance.getTypeName(), typedInstanceId,
                typeSystem.getDataType(TraitType.class, traitName).getAllSuperTypeNames());
        LOG.debug("created vertex {} for trait {}", traitInstanceVertex, traitName);

        // map all the attributes to this newly created vertex
        mapInstanceToVertex(typedInstanceId, traitInstance, traitInstanceVertex,
            traitInstance.fieldMapping().fields, idToVertexMap, false);

        // add an edge to the newly created vertex from the parent
        String relationshipLabel = GraphHelper.getTraitLabel(entityType.getName(), traitName);
        GraphHelper.addEdge(titanGraph, parentInstanceVertex, traitInstanceVertex, relationshipLabel);
    }

    private void mapPrimitiveToVertex(ITypedInstance typedInstance, Vertex instanceVertex,
        AttributeInfo attributeInfo) throws AtlasException {
        Object attrValue = typedInstance.get(attributeInfo.name);

        final String vertexPropertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        Object oldPropertyValue = instanceVertex.getProperty(vertexPropertyName);
        if (attrValue == null) {
            if (oldPropertyValue != null) {
                //Update to null
                LOG.info("Removing property value for " + vertexPropertyName);
                GraphHelper.setProperty(instanceVertex, vertexPropertyName, null);
            }
        } else {
            Object propertyValue = null;
            if (attributeInfo.dataType() == DataTypes.STRING_TYPE) {
                propertyValue = typedInstance.getString(attributeInfo.name);
            } else if (attributeInfo.dataType() == DataTypes.SHORT_TYPE) {
                propertyValue = typedInstance.getShort(attributeInfo.name);
            } else if (attributeInfo.dataType() == DataTypes.INT_TYPE) {
                propertyValue = typedInstance.getInt(attributeInfo.name);
            } else if (attributeInfo.dataType() == DataTypes.BIGINTEGER_TYPE) {
                propertyValue = typedInstance.getBigInt(attributeInfo.name);
            } else if (attributeInfo.dataType() == DataTypes.BOOLEAN_TYPE) {
                propertyValue = typedInstance.getBoolean(attributeInfo.name);
            } else if (attributeInfo.dataType() == DataTypes.BYTE_TYPE) {
                propertyValue = typedInstance.getByte(attributeInfo.name);
            } else if (attributeInfo.dataType() == DataTypes.LONG_TYPE) {
                propertyValue = typedInstance.getLong(attributeInfo.name);
            } else if (attributeInfo.dataType() == DataTypes.FLOAT_TYPE) {
                propertyValue = typedInstance.getFloat(attributeInfo.name);
            } else if (attributeInfo.dataType() == DataTypes.DOUBLE_TYPE) {
                propertyValue = typedInstance.getDouble(attributeInfo.name);
            } else if (attributeInfo.dataType() == DataTypes.BIGDECIMAL_TYPE) {
                propertyValue = typedInstance.getBigDecimal(attributeInfo.name);
            } else if (attributeInfo.dataType() == DataTypes.DATE_TYPE) {
                final Date dateVal = typedInstance.getDate(attributeInfo.name);
                //Convert Property value to Long  while persisting
                propertyValue = dateVal.getTime();
            }


            if (oldPropertyValue == null || !oldPropertyValue.equals(propertyValue)) {
                LOG.debug("Updating property value for " + vertexPropertyName);
                GraphHelper.setProperty(instanceVertex, vertexPropertyName, propertyValue);
            }
        }
    }
}
