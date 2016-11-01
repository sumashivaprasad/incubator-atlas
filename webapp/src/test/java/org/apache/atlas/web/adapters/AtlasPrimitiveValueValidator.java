package org.apache.atlas.web.adapters;

import org.apache.atlas.type.AtlasType;
import org.testng.Assert;

/**
 * Created by sshivaprasad on 10/31/16.
 */
public class AtlasPrimitiveValueValidator implements AtlasInstanceValueValidation {

    @Override
    public void assertIfEquals(final AtlasType type, final Object expected, final Object result) {
        Assert.assertEquals(expected, result);
    }
}
