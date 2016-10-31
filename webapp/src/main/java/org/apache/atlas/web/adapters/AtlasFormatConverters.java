package org.apache.atlas.web.adapters;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.typesystem.types.DataTypes;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.type.AtlasType.TypeCategory.ENTITY;
import static org.apache.atlas.type.AtlasType.TypeCategory.STRUCT;

@Singleton
public class AtlasFormatConverters {

    private AtlasFormatAdapter primitiveTypeConverter;
    private AtlasFormatAdapter mapFormatCoverter;
    private AtlasFormatAdapter listFormatCoverter;
    private AtlasFormatAdapter setFormatCoverter;

    private Map<Class, AtlasFormatAdapter> registry = new HashMap<>();

    public void registerConverter(AtlasFormatAdapter adapter) {
        if (adapter.getTypeCategory() == AtlasType.TypeCategory.PRIMITIVE) {
            this.primitiveTypeConverter = adapter;
        } else if (adapter.getTypeCategory() == AtlasType.TypeCategory.MAP) {
            this.mapFormatCoverter = adapter;
        } else if (adapter.getTypeCategory() == AtlasType.TypeCategory.ARRAY) {
            if ( adapter.getSourceType() == List.class) {
                listFormatCoverter = adapter;
            } else if (adapter.getSourceType() == Set.class) {
                setFormatCoverter = adapter;
            }
        } else {
            registry.put(adapter.getSourceType(), adapter);
        }
    }

    public AtlasFormatAdapter getConverter(Class sourceType) throws AtlasBaseException {
        if (isPrimitiveType(sourceType)) {
            return primitiveTypeConverter;
        } else if (isMapType(sourceType)) {
            return mapFormatCoverter;
        } else if (isArrayListType(sourceType)) {
            return listFormatCoverter;
        } else if (isSetType(sourceType)) {
            return setFormatCoverter;
        } else if (registry.containsKey(sourceType)) {
            return registry.get(sourceType);
        }
        throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Could not find the converter for this type " + sourceType);
    }

    public static boolean isArrayListType(Class c) {
        return List.class.isAssignableFrom(c);
    }

    public static boolean isSetType(Class c) {
        return Set.class.isAssignableFrom(c);
    }

    public static boolean isPrimitiveType(final Class c) {
        if (c != null) {
            if (Number.class.isAssignableFrom(c)) {
                return true;
            }

            if (String.class.isAssignableFrom(c)) {
                return true;
            }

            if (Date.class.isAssignableFrom(c)) {
                return true;
            }

            return c.isPrimitive();
        }
        return false;
    }

    public static boolean isMapType(Class c) {
        return Map.class.isAssignableFrom(c);
    }
}
