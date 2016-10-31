package org.apache.atlas.web.adapters;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

public class AtlasMapFormatConverter implements AtlasFormatAdapter<Map, Map> {

    protected AtlasTypeRegistry typeRegistry;
    protected AtlasFormatConverters registry;

    @Inject
    AtlasMapFormatConverter(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Inject
    public void init(AtlasFormatConverters registry) throws AtlasBaseException {
        this.registry = registry;
        registry.registerConverter(this);
    }

    @Override
    public Map convert(final Map source) throws AtlasBaseException {
       Map newMap = new HashMap<>();
       for (Object key : source.keySet()) {

           Object convertedKey = registry.getConverter(key.getClass());
           Object val = source.get(key);

           if ( val != null) {
               Object convertedValue = registry.getConverter(val.getClass());
               newMap.put(convertedKey, convertedValue);
           } else {
               newMap.put(convertedKey, val);
           }
       }

        return newMap;
    }

    @Override
    public Class getSourceType() {
        return Map.class;
    }

    @Override
    public Class getTargetType() {
        return Map.class;
    }

    @Override
    public AtlasType.TypeCategory getTypeCategory() {
        return AtlasType.TypeCategory.MAP;
    }
}

