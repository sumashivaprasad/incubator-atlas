package org.apache.atlas.repository.store.graph;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class EntityGraphDiscoveryContext {

    /**
     *  Keeps track of all the entities that need to be created/updated including its child entities *
     */
    private List<AtlasEntity> rootEntities = new ArrayList<>();

    //Key is a transient id/guid
    /**
     * These references have been resolved using a unique identifier like guid or a qualified name etc in Atlas repository
     */
    private Map<String, AtlasVertex> repositoryResolvedReferences = new LinkedHashMap<>();

    /**
     * Unresolved entity references
     */
    private List<AtlasEntity> unresolvedEntityReferences = new ArrayList<>();

    /**
     * Unresolved entity id references
     */
    private Set<AtlasObjectId> unresolvedIdReferences = new HashSet<>();

    public void addRepositoryResolvedReference(AtlasObjectId id, AtlasVertex vertex) {
        repositoryResolvedReferences.put(id.getGuid(), vertex);
    }

    public void addRepositoryResolvedReference(String id, AtlasVertex vertex) {
        repositoryResolvedReferences.put(id, vertex);
    }

    public void addUnResolvedEntityReference(AtlasEntity entity) {
        this.unresolvedEntityReferences.add(entity);
    }

    public void addUnResolvedIdReference(AtlasEntityType entityType, String id) {
        this.unresolvedIdReferences.add(new AtlasObjectId(entityType.getTypeName(), id));
    }

    public Set<AtlasObjectId> getUnresolvedIdReferences() {
        return unresolvedIdReferences;
    }

    public boolean isResolved(Object entity) {
        return repositoryResolvedReferences.containsKey(entity);
    }

    public AtlasVertex getResolvedReference(AtlasObjectId ref) {
        return repositoryResolvedReferences.get(ref.getGuid());
    }

    public Map<String, AtlasVertex> getRepositoryResolvedReferences() {
        return repositoryResolvedReferences;
    }

    public AtlasVertex getResolvedReference(String id) {
        return repositoryResolvedReferences.get(id);
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

    public boolean removeUnResolvedEntityReference(final AtlasObjectId id) {
        return unresolvedEntityReferences.remove(id);
    }

    public boolean removeUnResolvedEntityReferences(final List<AtlasEntity> entities) {
        return unresolvedEntityReferences.removeAll(entities);
    }

    public boolean removeUnResolvedIdReferences(final List<AtlasObjectId> entities) {
        return unresolvedEntityReferences.removeAll(entities);
    }

    public boolean removeUnResolvedIdReference(final AtlasObjectId entity) {
        return unresolvedIdReferences.remove(entity);
    }

    public boolean hasUnresolvedReferences() {
        return unresolvedEntityReferences.size() > 0 || unresolvedIdReferences.size() > 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        } else if (obj == this) {
            return true;
        } else if (obj.getClass() != getClass()) {
            return false;
        } else {
            EntityGraphDiscoveryContext ctx = (EntityGraphDiscoveryContext) obj;
            return Objects.equals(rootEntities, ctx.getRootEntities()) &&
                Objects.equals(repositoryResolvedReferences, ctx.getRepositoryResolvedReferences()) &&
                Objects.equals(unresolvedEntityReferences, ctx.getUnResolvedEntityReferences()) &&
                Objects.equals(unresolvedIdReferences, ctx.getUnresolvedIdReferences());
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(rootEntities, repositoryResolvedReferences, unresolvedEntityReferences, unresolvedIdReferences);
    }

    //TODO - toString
}
