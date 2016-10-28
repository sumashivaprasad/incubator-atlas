package org.apache.atlas.web.adapters;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.typesystem.types.DataTypes;

import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.type.AtlasType.TypeCategory.ENTITY;
import static org.apache.atlas.type.AtlasType.TypeCategory.STRUCT;

@Singleton
public class AtlasFormatters {

    private Map<AtlasType.TypeCategory, AtlasFormatAdapter> registry = new HashMap<>();

    public void registerConverter(AtlasType.TypeCategory sourceType, AtlasFormatAdapter adapter) {
        registry.put(sourceType, adapter);
    }

    public AtlasFormatAdapter getConverter(AtlasType.TypeCategory type) throws AtlasBaseException {
        if ( registry.containsKey(type) ) {
            return registry.get(type);
        }
        throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Could not find the converter for this type " + type);
    }
}
