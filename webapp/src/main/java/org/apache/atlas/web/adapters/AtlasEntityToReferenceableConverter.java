package org.apache.atlas.web.adapters;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;

import javax.inject.Inject;

public class AtlasEntityToReferenceableConverter implements AtlasFormatAdapter<AtlasEntity, Referenceable> {

    protected AtlasTypeRegistry typeRegistry;
    protected AtlasFormatConverters registry;

    @Inject
    AtlasEntityToReferenceableConverter(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Inject
    public void init(AtlasFormatConverters registry) throws AtlasBaseException {
        this.registry = registry;
        registry.registerConverter(this);
    }

    @Override
    public Referenceable convert(final AtlasEntity source) throws AtlasBaseException {
        Referenceable ref = new Referenceable(source.getTypeName());

        AtlasFormatAdapter structConverter = registry.getConverter(AtlasStruct.class);

        final Struct struct = (Struct) structConverter.convert(source);

        for (String attrName : source.getAttributes().keySet()) {
            ref.set(attrName, struct.get(attrName));
        }
        return ref;
    }

    @Override
    public Class getSourceType() {
        return AtlasEntity.class;
    }

    @Override
    public Class getTargetType() {
        return Referenceable.class;
    }

    @Override
    public AtlasType.TypeCategory getTypeCategory() {
        return AtlasType.TypeCategory.ENTITY;
    }
}
