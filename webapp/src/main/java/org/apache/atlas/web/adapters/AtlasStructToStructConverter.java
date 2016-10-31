package org.apache.atlas.web.adapters;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.Struct;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Singleton
public class AtlasStructToStructConverter implements AtlasFormatAdapter<AtlasStruct, Struct> {

    protected AtlasTypeRegistry typeRegistry;
    protected AtlasFormatConverters registry;

    @Inject
    AtlasStructToStructConverter(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Inject
    public void init(AtlasFormatConverters registry) throws AtlasBaseException {
        this.registry = registry;
        registry.registerConverter(this);
    }

    @Override
    public Struct convert(final AtlasStruct source) throws AtlasBaseException {

        Struct oldStruct = new Struct(source.getTypeName());

        final Map<String, Object> convertedAttributes = convertAttributes(source);

        for (String attrName : source.getAttributes().keySet()) {
            oldStruct.set(attrName, convertedAttributes.get(attrName));
        }

        return oldStruct;

    }

    @Override
    public Class getSourceType() {
        return AtlasStruct.class;
    }

    @Override
    public Class getTargetType() {
        return Struct.class;
    }

    @Override
    public AtlasType.TypeCategory getTypeCategory() {
        return AtlasType.TypeCategory.STRUCT;
    }

    protected Map<String, Object> convertAttributes(final AtlasStruct source) throws AtlasBaseException {
        Map<String, Object> newAttrMap = new HashMap<>();
        final Map<String, Object> attributes = source.getAttributes();

        for (String attrName : attributes.keySet()) {
            Object val = attributes.get(attrName);

            if ( val != null) {
                AtlasFormatAdapter converter = registry.getConverter(val.getClass());
                Object convertedVal = converter.convert(val);
                newAttrMap.put(attrName, convertedVal);
            } else {
                newAttrMap.put(attrName, null);
            }
        }

        return newAttrMap;
    }
}
