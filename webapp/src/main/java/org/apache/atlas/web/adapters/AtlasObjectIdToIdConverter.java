package org.apache.atlas.web.adapters;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntityId;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.persistence.Id;

import javax.inject.Inject;

public class AtlasObjectIdToIdConverter implements AtlasFormatAdapter<AtlasEntityId, Id> {

    protected AtlasTypeRegistry typeRegistry;
    protected AtlasFormatConverters registry;

    @Inject
    AtlasObjectIdToIdConverter(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Inject
    public void init(AtlasFormatConverters registry) throws AtlasBaseException {
        this.registry = registry;
        registry.registerConverter(this);
    }

    @Override
    public Id convert(final AtlasEntityId source) throws AtlasBaseException {
        Id ref = new Id(source.getGuid(), 0 , source.getTypeName(), source.getStatus().name());
        return ref;
    }

    @Override
    public Class getSourceType() {
        return AtlasEntityId.class;
    }

    @Override
    public Class getTargetType() {
        return Id.class;
    }

    @Override
    public AtlasType.TypeCategory getTypeCategory() {
        return AtlasType.TypeCategory.OBJECT_ID_TYPE;
    }
}
