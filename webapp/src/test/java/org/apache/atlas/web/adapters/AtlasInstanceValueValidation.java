package org.apache.atlas.web.adapters;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.type.AtlasType;

/**
 * Created by sshivaprasad on 10/31/16.
 */
public interface AtlasInstanceValueValidation<E, A> {

    void assertIfEquals(AtlasType type, E expected, A result) throws AtlasBaseException;
}
