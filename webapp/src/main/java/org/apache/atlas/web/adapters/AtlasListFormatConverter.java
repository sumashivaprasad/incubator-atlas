package org.apache.atlas.web.adapters;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.type.AtlasTypeRegistry;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AtlasListFormatConverter extends AtlasArrayFormatConverter {

    @Inject
    AtlasListFormatConverter(final AtlasTypeRegistry typeRegistry) {
        super(typeRegistry);
    }

    @Override
    public Object convert(final Object source) throws AtlasBaseException {
        List newList = new ArrayList();
        List originalList = (List) source;

        for (Object elem : originalList) {
            Object convertedKey = registry.getConverter(elem.getClass());
            newList.add(convertedKey);
        }

        return newList;
    }

    @Override
    public Class getSourceType() {
        return List.class;
    }

    @Override
    public Class getTargetType() {
        return List.class;
    }

}
