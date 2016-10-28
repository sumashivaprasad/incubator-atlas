/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityWithAssociations;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.instance.EntityMutationResponse;

import java.util.List;

/**
 * Persistence/Retrieval API for AtlasEntity
 */
public interface AtlasEntityStore {

    /**
     * Initialization
     */
    void init() throws AtlasBaseException;;


    /**
     * Create or update a list of entities
     * @param entities List of AtlasEntity objects that need to be created
     * @return AtlasEntity Return the entity that was created or updated if it already exists
     * @throws AtlasBaseException
     */

    List<EntityMutationResponse> createOrUpdate(List<AtlasEntity> entities) throws AtlasBaseException;


    /**
     * Create or update a list of entities
     * @param entities List of AtlasEntity objects that need to be created
     * @return AtlasEntity Return the entity that was created or updated if it already exists
     * @throws AtlasBaseException
     */

    List<EntityMutationResponse> partialUpdate(List<AtlasEntity> entities) throws AtlasBaseException;

    /**
     * @deprecated
     * Create or update a single entity
     * @param typeName The entity's type
     * @param attributeName Attribute that uniquely identifies the entity
     * @param attributeValue The unqiue attribute's value
     * @return AtlasEntity Return the entity that was created or updated if it already exists
     * @throws AtlasBaseException
     *
     */

    List<EntityMutationResponse> updateByUniqueAttribute(String typeName, String attributeName, String attributeValue, AtlasEntity entity) throws AtlasBaseException;

    /**
     *
     * Provides list of updated entity guids including any child entities
     * @param guid
     * @param entity
     * @return
     * @throws AtlasBaseException
     */
    List<EntityMutationResponse> updateById(String guid, AtlasEntity entity) throws AtlasBaseException;

    /**
     * Batch GET to retrieve entities by their ID
     * @param guid
     * @return
     * @throws AtlasBaseException
     */
    AtlasEntity.AtlasEntities getById(List<String> guid) throws AtlasBaseException;

    /**
     * Batch GET to retrieve entities and their associations by their ID
     * @param guid
     * @return
     * @throws AtlasBaseException
     */
    AtlasEntityWithAssociations getWithAssociationsById(List<String> guid) throws AtlasBaseException;

    /*
     * Return list of deleted entity guids
     */
    List<EntityMutationResponse> deleteById(List<String> guid) throws AtlasBaseException;

    /**
     * @deprecated
     * @param typeName
     * @param attributeName
     * @param attributeValue
     * @return
     * @throws AtlasBaseException
     */
    List<EntityMutationResponse> deleteByUniqueAttribute(String typeName, String attributeName, String attributeValue) throws AtlasBaseException;

    /**
     * Compose any type of mutation op - EntityMutation.EntityOperation - CREATE_OR_UPDATE, PARTIAL_UPDATE, DELETE etc in a single transaction
     * @param mutations
     * @return
     * @throws AtlasBaseException
     */
    List<EntityMutationResponse> batchMutate(EntityMutations mutations) throws AtlasBaseException;

    /**
     * Add classification(s)
     */
    void addClassifications(String guid, List<AtlasClassification> classification) throws AtlasBaseException;


    /**
     * Update classification(s)
     */
    void updateClassifications(String guid, List<AtlasClassification> classification) throws AtlasBaseException;

    /**
     * Delete classification(s)
     */
    void deleteClassifications(String guid, List<String> classificationNames) throws AtlasBaseException;

    /**
     *
     * Search by AND filters like typename, pre-defined attribute(s) eg: name, qualifiedName
     * @param searchFilter
     * @return
     * @throws AtlasBaseException
     */
    AtlasEntity.AtlasEntities searchEntities(SearchFilter searchFilter) throws AtlasBaseException;
}