package org.apache.atlas.repository.store.graph;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasVertex;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

/**
 * Created by sshivaprasad on 10/19/16.
 */
public final class DiscoveredEntities {

    List<AtlasEntity> rootEntities;

    Map<AtlasEntity, AtlasVertex> resolvedReferences = new LinkedHashMap<>();

    List<AtlasEntity> unresolvedReferences = new ArrayList<>();

    public void addResolvedReference(AtlasEntity entity, AtlasVertex vertex) {
        this.resolvedReferences.put(entity, vertex);
    }

    public void addUnResolvedReference(AtlasEntity entity) {
        this.unresolvedReferences.add(entity);
    }

    boolean isResolved(AtlasEntity entity) {
        return resolvedReferences.containsKey(entity);
    }

    AtlasVertex getVertex(AtlasEntity entity) {
        return resolvedReferences.get(entity);
    }

    public Set<AtlasEntity> getResolvedReferences() {
        return resolvedReferences.keySet();
    }

    public List<AtlasEntity> getUnResolvedReferences() {
        return unresolvedReferences;
    }

    public void setRootEntities(List<AtlasEntity> rootEntities) {
        this.rootEntities = rootEntities;
    }

    public List<AtlasEntity> getRootEntities() {
        return rootEntities;
    }

    public boolean removeUnResolvedReference(final AtlasEntity entity) {
        return unresolvedReferences.remove(entity);
    }

    public boolean hasDiscoveredEntities() {
        return unresolvedReferences.size() > 0;
    }
}
