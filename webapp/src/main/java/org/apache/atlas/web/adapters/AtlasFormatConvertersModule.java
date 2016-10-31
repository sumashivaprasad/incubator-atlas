package org.apache.atlas.web.adapters;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;

public class AtlasFormatConvertersModule extends AbstractModule {

  protected void configure() {
      Multibinder<AtlasFormatAdapter> multibinder
          = Multibinder.newSetBinder(binder(), AtlasFormatAdapter.class);
      multibinder.addBinding().to(AtlasStructToStructConverter.class).asEagerSingleton();
      multibinder.addBinding().to(AtlasEntityToReferenceableConverter.class).asEagerSingleton();
      multibinder.addBinding().to(AtlasObjectIdToIdConverter.class).asEagerSingleton();

      multibinder.addBinding().to(AtlasPrimitiveFormatConverter.class).asEagerSingleton();
      multibinder.addBinding().to(AtlasMapFormatConverter.class).asEagerSingleton();
      multibinder.addBinding().to(AtlasListFormatConverter.class).asEagerSingleton();
      multibinder.addBinding().to(AtlasSetFormatConverter.class).asEagerSingleton();
  }

}