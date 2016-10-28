package org.apache.atlas.web.adapters;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.Struct;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Singleton
public class AtlasStructToStructConverter implements AtlasFormatAdapter<AtlasStruct, Struct> {

    protected AtlasTypeRegistry typeRegistry;
    protected AtlasFormatters registry;

    @Inject
    AtlasStructToStructConverter(AtlasTypeRegistry typeRegistry, AtlasFormatters registry) {
        this.typeRegistry = typeRegistry;
        this.registry = registry;
    }

    @Inject
    public void init() throws AtlasBaseException {
        registry.registerConverter(AtlasType.TypeCategory.STRUCT, this);
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

    protected Map<String, Object> convertAttributes(final AtlasStruct source) throws AtlasBaseException {
        Map<String, Object> newAttrMap = new HashMap<>();
        final Map<String, Object> attributes = source.getAttributes();

        for (String attrName : attributes.keySet()) {
            Object val = attributes.get(attrName);

            if (isPrimitiveType(val)) {
                newAttrMap.put(attrName, attributes.get(attrName));
            }

            String typeName = source.getTypeName();
            AtlasType attrType = typeRegistry.getType(typeName);

            AtlasFormatAdapter converter = registry.getConverter(attrType.getTypeCategory());
            Object convertedVal = converter.convert(attributes.get(attrName));
            newAttrMap.put(attrName, convertedVal);
        }

        return newAttrMap;
    }

    private boolean isPrimitiveType(final Object o) {
        if (o != null) {
            if (Number.class.isAssignableFrom(o.getClass())) {
                return true;
            }

            if (String.class.isAssignableFrom(o.getClass())) {
                return true;
            }

            if (Date.class.isAssignableFrom(o.getClass())) {
                return true;
            }
        }
        return false;
    }
}
