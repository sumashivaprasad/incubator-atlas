package org.apache.atlas.repository.store.graph;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class DiscoveredEntities {

    private List<AtlasEntity> rootEntities;

    //Key can be a guid or an AtlasEntity with its qualifiedName
    private Map<Object, AtlasVertex> repositoryResolvedReferences = new LinkedHashMap<>();

    private List<AtlasEntity> unresolvedEntityReferences = new ArrayList<>();

    private List<AtlasObjectId> unresolvedIdReferences = new ArrayList<>();

    public void addRepositoryResolvedReference(Object entity, AtlasVertex vertex) {
        repositoryResolvedReferences.put(entity, vertex);
    }

    public void addUnResolvedEntityReference(AtlasEntity entity) {
        this.unresolvedEntityReferences.add(entity);
    }

    public void addUnResolvedIdReference(AtlasEntityType entityType, String id) {
        this.unresolvedIdReferences.add(new AtlasObjectId(entityType.getTypeName(), id));
    }

    public List<AtlasObjectId> getUnresolvedIdReferences() {
        return unresolvedIdReferences;
    }

    public boolean isResolved(Object entity) {
        return repositoryResolvedReferences.containsKey(entity);
    }

    AtlasVertex getResolvedVertex(AtlasEntity entity) {
        return repositoryResolvedReferences.get(entity);
    }

    public Map<Object, AtlasVertex> getResolvedReferences() {
        return repositoryResolvedReferences;
    }

    public AtlasVertex getResolvedReference(Object ref) {
        return repositoryResolvedReferences.get(ref);
    }

    public List<AtlasEntity> getUnResolvedEntityReferences() {
        return unresolvedEntityReferences;
    }

    public void setRootEntities(List<AtlasEntity> rootEntities) {
        this.rootEntities = rootEntities;
    }

    public void addRootEntity(AtlasEntity rootEntity) {
        this.rootEntities.add(rootEntity);
    }

    public List<AtlasEntity> getRootEntities() {
        return rootEntities;
    }

    public boolean removeUnResolvedEntityReference(final AtlasEntity entity) {
        return unresolvedEntityReferences.remove(entity);
    }

    public boolean removeUnResolvedEntityReferences(final List<AtlasEntity> entities) {
        return unresolvedEntityReferences.removeAll(entities);
    }

    public boolean removeUnResolvedIdReferences(final List<AtlasObjectId> entities) {
        return unresolvedEntityReferences.removeAll(entities);
    }

    public boolean hasUnresolvedReferences() {
        return unresolvedEntityReferences.size() > 0 || unresolvedIdReferences.size() > 0;
    }
}
