package org.apache.atlas.web.adapters;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;

import javax.inject.Inject;

public class AtlasEntityToReferenceableConverter implements AtlasFormatAdapter<AtlasEntity, Referenceable> {

    protected AtlasTypeRegistry typeRegistry;
    protected AtlasFormatters registry;

    @Inject
    AtlasEntityToReferenceableConverter(AtlasTypeRegistry typeRegistry, AtlasFormatters registry) {
        this.typeRegistry = typeRegistry;
        this.registry = registry;
    }

    @Inject
    public void init() throws AtlasBaseException {
       registry.registerConverter(AtlasType.TypeCategory.ENTITY, this);
    }

    @Override
    public Referenceable convert(final AtlasEntity source) throws AtlasBaseException {
        Referenceable ref = new Referenceable(source.getTypeName());

        AtlasFormatAdapter structConverter = registry.getConverter(AtlasType.TypeCategory.STRUCT);

        final Struct struct = (Struct) structConverter.convert(source);

        for (String attrName : source.getAttributes().keySet()) {
            ref.set(attrName, struct.get(attrName));
        }
        return ref;
    }
}
