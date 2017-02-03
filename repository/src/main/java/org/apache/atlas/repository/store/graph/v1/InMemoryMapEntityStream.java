package org.apache.atlas.repository.store.graph.v1;


import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class InMemoryMapEntityStream implements EntityStream {

    private Map<AtlasObjectId, AtlasEntity> entities = new HashMap<>();

    private Iterator<Map.Entry<AtlasObjectId, AtlasEntity>> iterator;

    public InMemoryMapEntityStream(Map<String, AtlasEntity> entityMap) {
        for (AtlasEntity entity : entityMap.values()) {
            entities.put(entity.getAtlasObjectId(), entity);
        }

        this.iterator = entities.entrySet().iterator();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public AtlasEntity next() {
        return iterator.hasNext() ? iterator.next().getValue() : null;
    }

    @Override
    public void reset() {
        iterator = entities.entrySet().iterator();
    }

    @Override
    public AtlasEntity getById(final AtlasObjectId id) {
        return entities.get(id);
    }
}
