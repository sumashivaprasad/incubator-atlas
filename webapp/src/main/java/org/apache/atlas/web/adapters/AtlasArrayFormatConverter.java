package org.apache.atlas.web.adapters;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;

import javax.inject.Inject;
import java.util.Collection;
import java.util.Map;

public abstract class AtlasArrayFormatConverter implements AtlasFormatAdapter {

    protected AtlasTypeRegistry typeRegistry;
    protected AtlasFormatConverters registry;

    @Inject
    AtlasArrayFormatConverter(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Inject
    public void init(AtlasFormatConverters registry) throws AtlasBaseException {
        this.registry = registry;
        registry.registerConverter(this);
    }

    @Override
    public AtlasType.TypeCategory getTypeCategory() {
        return AtlasType.TypeCategory.ARRAY;
    }
}

