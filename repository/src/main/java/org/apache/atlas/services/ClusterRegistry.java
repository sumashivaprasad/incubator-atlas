package org.apache.atlas.services;

import com.google.inject.Singleton;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.typesystem.Referenceable;

import java.util.HashMap;
import java.util.Map;

/**
 *  Singleton Registry of cluster instances which provides utility methods to
 *  look up cluster by their current or previous names
 */
public class ClusterRegistry {

    // Maintains a mapping between primary cluster names and their GUIDs
    private Map<String, String> clusterNameToIdMap = new HashMap<>();

    private Map<String, String> previousNamesToCurrentClusterMap = new HashMap<>();

    public static ClusterRegistry INSTANCE = new ClusterRegistry();

    private ClusterRegistry() {
    }

    public void registerCluster(Referenceable clusterEntity) {
        clusterNameToIdMap.put((String) clusterEntity.get(AtlasClient.NAME), clusterEntity.getId()._getId());

        Set<String> aliases = clusterEntity.get(AtlasClient.PREVIOUS_NAMES);
    }

    public void registerCluster(String name, String guid) {
        clusterNameToIdMap.put(name, guid);
    }

    public boolean exists(String name) {
        return clusterNameToIdMap.containsKey(name);
    }

    public String findIdByPrimaryName(String name) {
        return clusterNameToIdMap.get(name);
    }

    public String findIdByPreviousName(String prevName) {
        return previousNamesToCurrentClusterMap.containsKey(prevName) ? clusterNameToIdMap.get(previousNamesToCurrentClusterMap.get(prevName)) : null;
    }

    public void registerPreviousName(String primaryName, String previousName) {
        previousNamesToCurrentClusterMap.put(previousName, primaryName);
    }

}
