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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.SchemaViolationException;
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
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.ObjectGraphWalker;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.util.MurmurHash3;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.hash.MurmurHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TypedInstanceToGraphMapper {

    private static final Logger LOG = LoggerFactory.getLogger(TypedInstanceToGraphMapper.class);
    BiMap<Vertex, ITypedReferenceableInstance> vertexToInstanceMap = HashBiMap.create();
    private final Map<Id, Vertex> idToVertexMap = new HashMap<>();
    private final TypeSystem typeSystem = TypeSystem.getInstance();
    private final GraphToTypedInstanceMapper graphToTypedInstanceMapper;
//    private final String SIGNATURE_HASH_PROPERTY_KEY = Constants.INTERNAL_PROPERTY_KEY_PREFIX + "__signature";

    private static final GraphHelper graphHelper = GraphHelper.getInstance();

    public enum Operation {
        CREATE,
        UPDATE,
        DELETE
    }

    public TypedInstanceToGraphMapper(GraphToTypedInstanceMapper graphToTypedInstanceMapper) {
        this.graphToTypedInstanceMapper = graphToTypedInstanceMapper;
    }

    String[] mapTypedInstanceToGraph(Operation operation, ITypedReferenceableInstance... typedInstances)
        throws AtlasException {
        EntityProcessor entityProcessor = new EntityProcessor(operation);
        List<String> guids = new ArrayList<>();
        for (ITypedReferenceableInstance typedInstance : typedInstances) {
            List<ITypedReferenceableInstance> newTypedInstances = walkAndDiscoverClassInstances(entityProcessor, typedInstance);
            List<ITypedReferenceableInstance> instances =
                createVerticesForClassType(operation, newTypedInstances);

            addOrUpdateAttributesAndTraits(operation, instances);

            addFullTextProperty();

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
            entityProcessor.addInstanceIfNoExists(typedInstance);
        } catch (AtlasException me) {
            throw new RepositoryException("TypeSystem error when walking the ObjectGraph", me);
        }

        return discoverInstances(entityProcessor);
    }

    private void addOrUpdateAttributesAndTraits(Operation operation, List<ITypedReferenceableInstance> instances) throws AtlasException {
        for (ITypedReferenceableInstance instance : instances) {
            try {
                //new vertex, set all the properties
                addOrUpdateAttributesAndTraits(operation, instance);
            } catch (SchemaViolationException e) {
                throw new EntityExistsException(instance, e);
            }
        }
    }

    private List<ITypedReferenceableInstance> createVerticesForClassType(
        Operation op, List<ITypedReferenceableInstance> typedInstances) throws AtlasException {

        List<ITypedReferenceableInstance> instances = new ArrayList<>();
        for (ITypedReferenceableInstance typedInstance : typedInstances) {
            Id id = typedInstance.getId();
            if (!idToVertexMap.containsKey(id)) {
                Vertex instanceVertex;
                if (id.isAssigned()) {  // has a GUID
                    instanceVertex = graphHelper.getVertexForGUID(id.id);
                } else {
                    //Check if there is already an instance with the same unique attribute value
                    ClassType classType = typeSystem.getDataType(ClassType.class, typedInstance.getTypeName());
                    instanceVertex = graphHelper.getVertexForInstanceByUniqueAttribute(classType, typedInstance);
                    if (instanceVertex == null) {
                        instanceVertex = graphHelper.createVertexWithIdentity(typedInstance,
                            classType.getAllSuperTypeNames());

                        //Map only unique attributes
                        mapInstanceToVertex(id, typedInstance, instanceVertex,
                            classType.fieldMapping().fields, true);

                        if(Operation.CREATE.equals(op)) {
                            //Add only newly created vertices
                            instances.add(typedInstance);
                        }
                    }
                }

                if(Operation.UPDATE.equals(op)) {
                    //Add all identified vertices for further processing in case of an update
                    instances.add(typedInstance);
                }
                idToVertexMap.put(id, instanceVertex);
                vertexToInstanceMap.put(instanceVertex, typedInstance);
            }
        }
        return instances;
    }

    public String updateGraphByUniqueAttribute(String uniqueAttrName, Object uniqAttrValue, ITypedReferenceableInstance typedInstance)
        throws AtlasException {
        EntityProcessor entityProcessor = new EntityProcessor(Operation.UPDATE);
        ITypedReferenceableInstance instanceUpdated =
            updateByUniqueAttribute(uniqueAttrName, uniqAttrValue, typedInstance);

        List<ITypedReferenceableInstance> newTypedInstances = walkAndDiscoverClassInstances(entityProcessor, instanceUpdated);
        List<ITypedReferenceableInstance> instances =
            createVerticesForClassType(Operation.UPDATE, newTypedInstances);

        addOrUpdateAttributesAndTraits(Operation.UPDATE, instances);

        addFullTextProperty();

        //Return guid for
        Vertex instanceVertex = vertexToInstanceMap.inverse().get(typedInstance);
        return instanceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
    }

    private ITypedReferenceableInstance updateByUniqueAttribute(
        String uniqeAttrName, Object attrValue, ITypedReferenceableInstance typedInstance) throws AtlasException {

        final Id id = typedInstance.getId();
        ClassType classType = typeSystem.getDataType(ClassType.class, typedInstance.getTypeName());
        final String propertyName = GraphHelper.getQualifiedFieldName(classType, uniqeAttrName);
        Vertex instanceVertex = graphHelper.getVertexForProperty(propertyName, attrValue);
        //Check if there is already an instance with the given unique attribute value
        if (instanceVertex == null) {
            throw new EntityNotFoundException(String.format("Entity with unique attribute(%s, %s) not found", uniqeAttrName, attrValue));
        }
        mapInstanceToVertex(id, typedInstance, instanceVertex, classType.fieldMapping().fields, true);
        return typedInstance;
    }

    private void addToGuids(ITypedReferenceableInstance typedInstance, List<String> guids) {
        Vertex instanceVertex = idToVertexMap.get(typedInstance.getId());
        String guid = instanceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
        guids.add(guid);
    }

    private void addFullTextProperty() throws AtlasException {
        FullTextMapper fulltextMapper = new FullTextMapper(graphToTypedInstanceMapper);
        for (ITypedReferenceableInstance typedInstance : vertexToInstanceMap.values()) { // Traverse
            Vertex instanceVertex = idToVertexMap.get(typedInstance.getId());
            String fullText = fulltextMapper.mapRecursive(instanceVertex, true);
            GraphHelper.setProperty(instanceVertex, Constants.ENTITY_TEXT_PROPERTY_KEY, fullText);
        }
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
        for (IReferenceableInstance transientInstance : entityProcessor.getInstances()) {
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


    private void addOrUpdateAttributesAndTraits(Operation operation, ITypedReferenceableInstance typedInstance)
        throws AtlasException {
        LOG.debug("Adding/Updating typed instance {}", typedInstance.getTypeName());

        Id id = typedInstance.getId();
        if (id == null) { // oops
            throw new RepositoryException("id cannot be null");
        }

        Vertex instanceVertex = idToVertexMap.get(id);

        // add the attributes for the instance
        ClassType classType = typeSystem.getDataType(ClassType.class, typedInstance.getTypeName());
        final Map<String, AttributeInfo> fields = classType.fieldMapping().fields;

        mapInstanceToVertex(id, typedInstance, instanceVertex, fields, false);

        if (Operation.CREATE.equals(operation)) {
            //TODO - Handle Trait updates
            addTraits(typedInstance, instanceVertex, classType);
        }
    }

    private void addTraits(ITypedReferenceableInstance typedInstance, Vertex instanceVertex, ClassType classType) throws AtlasException {
        for (String traitName : typedInstance.getTraits()) {
            LOG.debug("mapping trait {}", traitName);
            GraphHelper.addProperty(instanceVertex, Constants.TRAIT_NAMES_PROPERTY_KEY, traitName);
            ITypedStruct traitInstance = (ITypedStruct) typedInstance.getTrait(traitName);

            // add the attributes for the trait instance
            mapTraitInstanceToVertex(traitInstance, typedInstance.getId(), classType, instanceVertex);
        }
    }

    private void mapInstanceToVertex(Id id, ITypedInstance typedInstance, Vertex instanceVertex,
        Map<String, AttributeInfo> fields, boolean mapOnlyUniqueAttributes)
        throws AtlasException {
        LOG.debug("Mapping instance {} of {} to vertex {}", typedInstance, typedInstance.getTypeName(),
            instanceVertex);
        for (AttributeInfo attributeInfo : fields.values()) {
            if (mapOnlyUniqueAttributes && !attributeInfo.isUnique) {
                continue;
            }
            final IDataType dataType = attributeInfo.dataType();
            mapAttributesToVertex(id, typedInstance, instanceVertex, attributeInfo, dataType);
        }
    }

    void mapAttributesToVertex(Id id, ITypedInstance typedInstance, Vertex instanceVertex,
        AttributeInfo attributeInfo, IDataType dataType) throws AtlasException {
        Object attrValue = typedInstance.get(attributeInfo.name);
        LOG.debug("mapping attribute {} = {}", attributeInfo.name, attrValue);
        final String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        String edgeLabel = GraphHelper.getEdgeLabel(typedInstance, attributeInfo);
//        if (attrValue == null) {
//            return;
//        }

        switch (dataType.getTypeCategory()) {
        case PRIMITIVE:
            mapPrimitiveToVertex(typedInstance, instanceVertex, attributeInfo);
            break;

        case ENUM:
            //handles both int and string for enum
            Object oldPropertyValue = instanceVertex.getProperty(attributeInfo.name);
            if (typedInstance.get(attributeInfo.name) == null && oldPropertyValue != null) {
                //Remove attribute value if current value is not null and new value is set to null
                GraphHelper.setProperty(instanceVertex, propertyName, null);
            } else if (typedInstance.get(attributeInfo.name) != null) {
                EnumValue enumValue =
                    (EnumValue) dataType.convert(typedInstance.get(attributeInfo.name), Multiplicity.REQUIRED);

                if (oldPropertyValue == null || !oldPropertyValue.equals(enumValue.value)) {
                    //Reset value only if theres a change
                    GraphHelper.setProperty(instanceVertex, propertyName, enumValue.value);
                }
            }
            break;

        case ARRAY:
            mapArrayCollectionToVertex(id, typedInstance, instanceVertex, attributeInfo);
            break;

        case MAP:
            mapMapCollectionToVertex(id, typedInstance, instanceVertex, attributeInfo);
            break;

        case STRUCT:
            Iterable<Edge> outGoingEdgesByLabel = GraphHelper.getOutGoingEdgesByLabel(instanceVertex, edgeLabel);
            if (outGoingEdgesByLabel.iterator().hasNext()) {
                updateStructVertex(id, (ITypedStruct) typedInstance.get(attributeInfo.name), instanceVertex, attributeInfo, attributeInfo.dataType(), outGoingEdgesByLabel.iterator().next());
            } else {
                addStructVertex(id, (ITypedStruct) typedInstance.get(attributeInfo.name), instanceVertex, attributeInfo, edgeLabel);
            }
            break;
        case TRAIT:
            // do NOTHING - this is taken care of earlier
            break;

        case CLASS:
            outGoingEdgesByLabel = GraphHelper.getOutGoingEdgesByLabel(instanceVertex, edgeLabel);
            Vertex toVertex = getClassVertex((ITypedReferenceableInstance) typedInstance.get(attributeInfo.name));
            if(toVertex == null && typedInstance.get(attributeInfo.name) != null) {
                LOG.error("Could not find vertex for Class Reference " + typedInstance.get(attributeInfo.name));
                throw new EntityNotFoundException("Could not find vertex for Class Reference " + typedInstance.get(attributeInfo.name));
            } else {
                if (outGoingEdgesByLabel.iterator().hasNext()) {
                    Id classRefId = getId((ITypedReferenceableInstance) typedInstance.get(attributeInfo.name));
                    updateClassEdge(classRefId, (ITypedReferenceableInstance) typedInstance.get(attributeInfo.name), instanceVertex, outGoingEdgesByLabel.iterator().next(), toVertex, attributeInfo, attributeInfo.dataType(), edgeLabel);
                } else if(typedInstance.get(attributeInfo.name) != null) {
                    addClassEdge(instanceVertex, toVertex, edgeLabel);
                }
            }
            break;
        default:
            throw new IllegalArgumentException("Unknown type category: " + dataType.getTypeCategory());
        }
    }

    /******************************************** STRUCT **************************************************/

    private Pair<Vertex, Edge> updateStructVertex(Id id, ITypedStruct typedInstance, Vertex instanceVertex, AttributeInfo attributeInfo, IDataType elemType, Edge relEdge) throws AtlasException {
        //Already existing vertex. Update
        Vertex structInstanceVertex = relEdge.getVertex(Direction.IN);

        if (typedInstance == null) {
            //Delete edge and remove struct vertex since struct attribute has been set to null
            removeUnusedReference(relEdge.getId().toString(), attributeInfo, elemType);
        } else {
            // Update attributes
            // map all the attributes to this vertex
//            int newSignature = hash(typedInstance);
//            String curSignature = structInstanceVertex.getProperty(SIGNATURE_HASH_PROPERTY_KEY);
//            if(newSignature != Integer.parseInt(curSignature)) {
                //Update struct vertex instance only if there is a change
                mapInstanceToVertex(id, typedInstance, structInstanceVertex, typedInstance.fieldMapping().fields, false);
//                GraphHelper.setProperty(structInstanceVertex, SIGNATURE_HASH_PROPERTY_KEY, String.valueOf(newSignature));
//            } else {
//                LOG.debug("Skipping update of struct vertex since signature matches for " + id + ":" + typedInstance);
//            }
        }
        return Pair.of(structInstanceVertex, relEdge);
    }

    private Pair<Vertex, Edge> addStructVertex(Id id, ITypedStruct typedInstance, Vertex instanceVertex, AttributeInfo attributeInfo, String edgeLabel) throws AtlasException {
        if (typedInstance == null) {
            return null;
        }
        // add a new vertex for the struct or trait instance
        Vertex structInstanceVertex = graphHelper
            .createVertexWithoutIdentity(typedInstance.getTypeName(), id,
                Collections.<String>emptySet()); // no super types for struct type
        LOG.debug("created vertex {} for struct {} value {}", structInstanceVertex, attributeInfo.name, typedInstance);

        // map all the attributes to this new vertex
        mapInstanceToVertex(id, typedInstance, structInstanceVertex, typedInstance.fieldMapping().fields, false);
        // add an edge to the newly created vertex from the parent
        Edge relEdge = graphHelper.addEdge(instanceVertex, structInstanceVertex, edgeLabel);
//        int signature = hash(typedInstance);
//        graphHelper.setProperty(structInstanceVertex, SIGNATURE_HASH_PROPERTY_KEY, String.valueOf(signature));

        return Pair.of(structInstanceVertex, relEdge);
    }

    /******************************************** ARRAY **************************************************/

    private void mapArrayCollectionToVertex(Id id, ITypedInstance typedInstance, Vertex instanceVertex,
        AttributeInfo attributeInfo) throws AtlasException {
        LOG.debug("Mapping instance {} to vertex {} for name {}", typedInstance.getTypeName(), instanceVertex,
            attributeInfo.name);
//        List list = (List) typedInstance.get(attributeInfo.name);
//        if (list == null || list.isEmpty()) {
//            return;
//        }

        String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        IDataType elementType = ((DataTypes.ArrayType) attributeInfo.dataType()).getElemType();
        List<String> currentEntries = instanceVertex.getProperty(propertyName);
        List<String> newEntries = null;

        if (currentEntries == null || currentEntries.isEmpty()) {
            newEntries = createArrayCollection(id, typedInstance, instanceVertex, attributeInfo, elementType);
        } else {
            newEntries = updateArrayCollection(id, typedInstance, instanceVertex, attributeInfo, elementType, currentEntries);
        }

        // for dereference on way out
        GraphHelper.setProperty(instanceVertex, propertyName, newEntries);
    }

    private List<String> updateArrayCollection(Id id, ITypedInstance typedInstance, Vertex instanceVertex,
        AttributeInfo attributeInfo, IDataType elementType, List<String> curEntries) throws AtlasException {
        String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        final String edgeLabel = GraphHelper.EDGE_LABEL_PREFIX + propertyName;

        List list = (List) typedInstance.get(attributeInfo.name);
        List<String> values = new ArrayList<>();
        if(list !=  null) {
            for (int index = 0; index < list.size(); index++) {
                String entryId =
                    updateOrGetCollectionEntry(id, instanceVertex, attributeInfo, elementType,
                        list.get(index), index < curEntries.size() ? curEntries.get(index) : null, edgeLabel);
                if (entryId != null) {
                    values.add(entryId);
                } else {
                    LOG.warn(String.format("Collection entry mapped is null for id:attribute (%s:%s)", id, attributeInfo));
                }
            }
        } else {
            //Remove all edges since list is updated to null
            for(String edgeId : curEntries) {
                removeUnusedReference(edgeId, attributeInfo, elementType);
            }
        }
        return values;
    }

    private List<String> createArrayCollection(Id id, ITypedInstance typedInstance, Vertex instanceVertex,
        AttributeInfo attributeInfo, IDataType elementType) throws AtlasException {
        String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        final String edgeLabel = GraphHelper.EDGE_LABEL_PREFIX + propertyName;

        List list = (List) typedInstance.get(attributeInfo.name);
        List<String> values = new ArrayList<>();
        if (list != null) {
            for (int index = 0; index < list.size(); index++) {
                String entryId =
                    addOrGetCollectionEntry(id, instanceVertex, attributeInfo, elementType,
                        list.get(index), edgeLabel);
                if (entryId != null) {
                    values.add(entryId);
                } else {
                    LOG.warn(String.format("Collection entry mapped is null for id:attribute (%s:%s)", id, attributeInfo));
                }
            }
        }
        return values;
    }

    /******************************************** MAP **************************************************/

    private void mapMapCollectionToVertex(Id id, ITypedInstance typedInstance, Vertex instanceVertex,
        AttributeInfo attributeInfo) throws AtlasException {
        LOG.debug("Mapping instance {} to vertex {} for name {}", typedInstance.getTypeName(), instanceVertex,
            attributeInfo.name);
        @SuppressWarnings("unchecked") Map<Object, Object> collection =
            (Map<Object, Object>) typedInstance.get(attributeInfo.name);
//        if (collection == null || collection.isEmpty()) {
//            return;
//        }

        if (collection == null) {
            collection = new HashMap<>();
        }

        String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        IDataType elementType = ((DataTypes.MapType) attributeInfo.dataType()).getValueType();

        for (Map.Entry entry : collection.entrySet()) {
            String myPropertyName = propertyName + "." + entry.getKey().toString();
            final String edgeLabel = GraphHelper.EDGE_LABEL_PREFIX + myPropertyName;

            String currentEntry = instanceVertex.getProperty(myPropertyName);
            String value = null;
            if (currentEntry == null || currentEntry.isEmpty()) {
                value = addOrGetCollectionEntry(id, instanceVertex, attributeInfo, elementType,
                    entry.getValue(), edgeLabel);
            } else {
                value = updateOrGetCollectionEntry(id, instanceVertex, attributeInfo, elementType,
                        entry.getValue(), currentEntry, edgeLabel);

            }
            //Add/Update/Remove property value
            GraphHelper.setProperty(instanceVertex, myPropertyName, value);
        }

        //Remove unused keys
        List<Object> origKeys = instanceVertex.getProperty(propertyName);
        if (origKeys != null) {
            if (collection != null) {
                origKeys.removeAll(collection.keySet());
            }
            for (Object unusedKey : origKeys) {
                String edgeLabel = GraphHelper.EDGE_LABEL_PREFIX + propertyName + "." + unusedKey.toString();
                if (instanceVertex.getEdges(Direction.OUT, edgeLabel).iterator().hasNext()) {
                    Edge edge = instanceVertex.getEdges(Direction.OUT, edgeLabel).iterator().next();
                    removeUnusedReference(edge.getId().toString(), attributeInfo, ((DataTypes.MapType) attributeInfo.dataType()).getValueType());
                }
            }
        }

        // for dereference on way out
        GraphHelper.setProperty(instanceVertex, propertyName, new ArrayList(collection.keySet()));
    }

    /******************************************** ARRAY & MAP **************************************************/

    private String addOrGetCollectionEntry(Id id, Vertex instanceVertex, AttributeInfo attributeInfo,
        IDataType elementType, Object value, String edgeLabel)
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
            Pair<Vertex, Edge> vertexEdgePair = addStructVertex(id, structAttr, instanceVertex, attributeInfo, edgeLabel);
            return (vertexEdgePair != null) ? vertexEdgePair.getRight().getId().toString() : null;

        case CLASS:
            Vertex toVertex = getClassVertex((ITypedReferenceableInstance) value);
            if(toVertex == null && value != null) {
                LOG.error("Could not find vertex for Class Reference " + value);
                throw new EntityNotFoundException("Could not find vertex for Class Reference " + value);
            } else if (value != null) {
                vertexEdgePair = addClassEdge(instanceVertex, toVertex, edgeLabel);
                return (vertexEdgePair != null) ? vertexEdgePair.getRight().getId().toString() : null;
            }
        default:
            throw new IllegalArgumentException("Unknown type category: " + elementType.getTypeCategory());
        }
    }

    private String updateOrGetCollectionEntry(Id id, Vertex instanceVertex, AttributeInfo attributeInfo,
        IDataType elementType, Object newVal, String curVal, String edgeLabel)
        throws AtlasException {

        switch (elementType.getTypeCategory()) {
        case PRIMITIVE:
        case ENUM:
            return newVal != null ? newVal.toString() : null;

        case ARRAY:
        case MAP:
        case TRAIT:
            // do nothing
            return null;

        case STRUCT:
            ITypedStruct structAttr = (ITypedStruct) newVal;
            Pair<Vertex, Edge> vertexEdgePair = null;
            if (curVal != null) {
                Edge edge = graphHelper.getOutGoingEdgeById(curVal);
                vertexEdgePair = updateStructVertex(id, structAttr, instanceVertex, attributeInfo, elementType, edge);
            } else {
                vertexEdgePair = addStructVertex(id, structAttr, instanceVertex, attributeInfo, edgeLabel);
            }

            return (vertexEdgePair != null) ? vertexEdgePair.getRight().getId().toString() : null;

        case CLASS:
            Vertex toVertex = getClassVertex((ITypedReferenceableInstance) newVal);
            if(toVertex == null && newVal != null) {
                LOG.error("Could not find vertex for Class Reference " + newVal);
                throw new EntityNotFoundException("Could not find vertex for Class Reference " + newVal);
            } else {
                if (curVal != null) {
                    Edge edge = graphHelper.getOutGoingEdgeById(curVal);
                    Id classRefId = getId((ITypedReferenceableInstance) newVal);
                    vertexEdgePair = updateClassEdge(classRefId, (ITypedReferenceableInstance) newVal, instanceVertex, edge, toVertex, attributeInfo, elementType, edgeLabel);
                } else {
                    vertexEdgePair = addClassEdge(instanceVertex, toVertex, edgeLabel);
                }
                return vertexEdgePair.getRight().getId().toString();
            }
        default:
            throw new IllegalArgumentException("Unknown type category: " + elementType.getTypeCategory());
        }
    }

    /******************************************** CLASS **************************************************/

    private Pair<Vertex, Edge> addClassEdge(Vertex instanceVertex, Vertex toVertex, String edgeLabel) throws AtlasException {
            // add an edge to the class vertex from the instance
          Edge edge = graphHelper.addEdge(instanceVertex, toVertex, edgeLabel);
          return Pair.of(toVertex, edge);
    }

    private Vertex getClassVertex(ITypedReferenceableInstance typedReference) throws EntityNotFoundException {
        Vertex referenceVertex = null;
        Id id = null;
        if (typedReference != null) {
            id = typedReference instanceof Id ? (Id) typedReference : typedReference.getId();
            if (id.isAssigned()) {
                referenceVertex = graphHelper.getVertexForGUID(id.id);
            } else {
                referenceVertex = idToVertexMap.get(id);
            }
        }

        return referenceVertex;
    }

    private Id getId(ITypedReferenceableInstance typedReference) throws EntityNotFoundException {
        Id id = null;
        if (typedReference != null) {
            id = typedReference instanceof Id ? (Id) typedReference : typedReference.getId();
        }

        if (id.isUnassigned()) {
            Vertex classVertex = idToVertexMap.get(id);
            String guid = classVertex.getProperty(Constants.GUID_PROPERTY_KEY);
            id = new Id(guid, 0, typedReference.getTypeName());
        }
        return id;
    }


    private Pair<Vertex, Edge> updateClassEdge(Id id, ITypedReferenceableInstance typedInstance, Vertex instanceVertex, Edge edge, Vertex toVertex, AttributeInfo attributeInfo, IDataType dataType, String edgeLabel) throws AtlasException {
        Pair<Vertex, Edge> result = Pair.of(toVertex, edge);
        Edge newEdge = edge;
        // Update edge if it exists
        Vertex invertex = edge.getVertex(Direction.IN);
        String currentGUID = invertex.getProperty(Constants.GUID_PROPERTY_KEY);
        Id currentId = new Id(currentGUID, 0, (String) invertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY));
        if (!currentId.equals(id)) {
             // add an edge to the class vertex from the instance
            if(toVertex != null) {
                newEdge = graphHelper.addEdge(instanceVertex, toVertex, edgeLabel);
                result = Pair.of(toVertex, newEdge);
            }
            removeUnusedReference(edge.getId().toString(), attributeInfo, dataType);
        }

        if (attributeInfo.isComposite) {
            //Update the attributes also if composite
            if (typedInstance.fieldMapping() != null) {
                //In case of Id instance, fieldMapping is null
                mapInstanceToVertex(id, typedInstance, toVertex, typedInstance.fieldMapping().fields , false);
            }
        }

        return result;
    }

    /******************************************** TRAITS ****************************************************/

    void mapTraitInstanceToVertex(ITypedStruct traitInstance, Id typedInstanceId,
        IDataType entityType, Vertex parentInstanceVertex)
        throws AtlasException {
        // add a new vertex for the struct or trait instance
        final String traitName = traitInstance.getTypeName();
        Vertex traitInstanceVertex = graphHelper
            .createVertexWithoutIdentity(traitInstance.getTypeName(), typedInstanceId,
                typeSystem.getDataType(TraitType.class, traitName).getAllSuperTypeNames());
        LOG.debug("created vertex {} for trait {}", traitInstanceVertex, traitName);

        // map all the attributes to this newly created vertex
        mapInstanceToVertex(typedInstanceId, traitInstance, traitInstanceVertex,
            traitInstance.fieldMapping().fields, false);

        // add an edge to the newly created vertex from the parent
        String relationshipLabel = GraphHelper.getTraitLabel(entityType.getName(), traitName);
        graphHelper.addEdge(parentInstanceVertex, traitInstanceVertex, relationshipLabel);
    }

    /******************************************** PRIMITIVES **************************************************/

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
                LOG.debug("Setting property value for " + vertexPropertyName);
                GraphHelper.setProperty(instanceVertex, vertexPropertyName, propertyValue);
            }
        }
    }

    private Edge removeUnusedReference(String edgeId, AttributeInfo attributeInfo, IDataType<?> elementType) {
        //Remove edges for property values which do not exist any more
        Edge removedRelation = null;
        switch (elementType.getTypeCategory()) {
        case STRUCT:
            removedRelation = graphHelper.removeRelation(edgeId, true);
            //Remove the vertex from state so that further processing no longer uses this
            vertexToInstanceMap.remove(removedRelation.getVertex(Direction.IN));
            break;
        case CLASS:
            removedRelation = graphHelper.removeRelation(edgeId, attributeInfo.isComposite);
            if (attributeInfo.isComposite) {
                //Remove the vertex from state so that further processing no longer uses this
                vertexToInstanceMap.remove(removedRelation.getVertex(Direction.IN));
            }
            break;
        }
        return removedRelation;
    }

//    static int hash(ITypedReferenceableInstance classInstance) throws AtlasException {
//        if (classInstance instanceof ReferenceableInstance) {
//            int result = classInstance.getTypeName().hashCode();
//            result = 31 * result + classInstance.getId().hashCode();
//            Map<String, Object> values = classInstance.getValuesMap();
//            result = 31 * result + values.hashCode();
//            return result;
//        } else {
//            return ((Id) classInstance).hashCode();
//        }
//    }
//
//    static int hash(ITypedStruct structInstance) throws AtlasException {
//        int result = structInstance.getTypeName().hashCode();
//        Map<String, Object> values = structInstance.getValuesMap();
//        result = 31 * result + values.hashCode();
//        return result;
//    }

//    static int hash(ITypedReferenceableInstance classInstance, boolean isRecursive) throws AtlasException {
//        if (classInstance instanceof ReferenceableInstance) {
//            int result = classInstance.getTypeName().hashCode();
//            result = 31 * result + classInstance.getId().hashCode();
//            Map<String, Object> values = classInstance.getValuesMap();
//            result = 31 * result + values.hashCode();
//            return result;
//        } else {
//            return ((Id) classInstance).hashCode();
//        }
//    }
//
//    static String hash(ITypedStruct structInstance) throws AtlasException {
//        final MessageDigest digester = MD5Hash.getDigester();
//        digester.update(structInstance.getTypeName().getBytes());
////        Map<String, Object> values = structInstance.getValuesMap();
//        for(AttributeInfo aInfo : structInstance.fieldMapping().fields) {
//            final Object val = structInstance.get(aInfo.name);
//            if(val != null) {
//                digester.update(val);
//            }
//            result = 31 * result + values.hashCode();
//        }
//        return result;
//    }
//
//    public static void main(String[] args) {
//        System.out.println("hash1 = " + "FB".hashCode());
//        System.out.println("hash2 = " + "Ea".hashCode());
//
//        System.out.println("hash3 = " + MurmurHash3.getInstance().hash("FB".getBytes()));
//        System.out.println("hash4 = " + MurmurHash3.getInstance().hash("Ea".getBytes()));
//
//        System.out.println("hash md5 = " + MD5Hash.digest("FB".getBytes()));
//        System.out.println("hash md5 = " + MD5Hash.digest("Ea".getBytes()));
//    }
}
