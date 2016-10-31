package org.apache.atlas.web.adapters;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.typesystem.Struct;

public interface AtlasFormatAdapter<S, T> {

    T convert(S source) throws AtlasBaseException;

    Class getSourceType();

    Class getTargetType();

    AtlasType.TypeCategory getTypeCategory();
}
