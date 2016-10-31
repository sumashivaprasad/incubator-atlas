package org.apache.atlas.web.adapters;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

public class AtlasPrimitiveFormatConverter implements AtlasFormatAdapter {

    protected AtlasTypeRegistry typeRegistry;
    protected AtlasFormatConverters registry;

    @Inject
    AtlasPrimitiveFormatConverter(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Inject
    public void init(AtlasFormatConverters registry) throws AtlasBaseException {
        this.registry = registry;
        registry.registerConverter(this);
    }

    @Override
    public Object convert(final Object source) throws AtlasBaseException {
       return source;
    }

    @Override
    public Class getSourceType() {
        return Object.class;
    }

    @Override
    public Class getTargetType() {
        return Object.class;
    }

    @Override
    public AtlasType.TypeCategory getTypeCategory() {
        return AtlasType.TypeCategory.PRIMITIVE;
    }
}

