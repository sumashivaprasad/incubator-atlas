package org.apache.atlas.web.adapters;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.type.AtlasTypeRegistry;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class AtlasSetFormatConverter extends AtlasArrayFormatConverter {

    @Inject
    AtlasSetFormatConverter(final AtlasTypeRegistry typeRegistry) {
        super(typeRegistry);
    }

    @Override
    public Object convert(final Object source) throws AtlasBaseException {
        Set newSet = new LinkedHashSet();
        Set origSet = (Set) source;

        for (Object elem : origSet) {
            Object convertedKey = registry.getConverter(elem.getClass());
            newSet.add(convertedKey);
        }

        return newSet;
    }

    @Override
    public Class getSourceType() {
        return Set.class;
    }

    @Override
    public Class getTargetType() {
        return Set.class;
    }

}
