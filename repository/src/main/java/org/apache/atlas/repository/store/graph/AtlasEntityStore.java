package org.apache.atlas.repository.store.graph;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;

import java.util.List;

public interface AtlasEntityStore {

    void init();

    List<AtlasEntity> createEntites(List<AtlasEntity> entities) throws AtlasBaseException;

    AtlasEntity createEntity(AtlasEntity entity) throws AtlasBaseException;

    List<AtlasEntity> createOrUpdateEntites(List<AtlasEntity> entities) throws AtlasBaseException;

    AtlasEntity createOrUpdateEntity(AtlasEntity entity) throws AtlasBaseException;

    List<AtlasEntity> updateByUniqueAttribute(String attribute, AtlasEntity entity) throws AtlasBaseException;

    AtlasEntity getEntityById(String guid) throws AtlasBaseException;

    AtlasEntity getEntityByUniqueAttribute(String attribute) throws AtlasBaseException;

     /*
      * Return list of deleted entities
      */
    List<String> deleteEntitiesById(List<AtlasEntity> entities) throws AtlasBaseException;

    List<String> deleteEntitiesByUniqueAttribute(List<AtlasEntity> entities) throws AtlasBaseException;

}
