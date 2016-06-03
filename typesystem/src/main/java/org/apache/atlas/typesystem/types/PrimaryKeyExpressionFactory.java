/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.typesystem.types;

import org.apache.atlas.AtlasException;

import java.util.HashMap;
import java.util.Map;

public class PrimaryKeyExpressionFactory<T extends PrimaryKeyExpression> {

    public static final PrimaryKeyExpressionFactory instance = new PrimaryKeyExpressionFactory();

    public final T JEXL_EXPRESSION = (T) new JEXLExpression();

    private Map<String, T> exprHandlers = new HashMap<>();

    private PrimaryKeyExpressionFactory() {
        exprHandlers.put(JEXL_EXPRESSION.type(), JEXL_EXPRESSION);
    }

    public void registerExpressionHandler(String type, Class<T> expressionHandler) throws AtlasException {
        try {
            exprHandlers.put(type, expressionHandler.newInstance());
        } catch (InstantiationException e) {
            throw new AtlasException(e);
        } catch (IllegalAccessException e) {
            throw new AtlasException(e);
        }
    }

    public T byType(String type) {
        if (exprHandlers.containsKey(type)) {
            return exprHandlers.get(type);
        }
        throw new IllegalArgumentException("Unrecognized type " + type);
    }

    public T getDefaultHandler() {
        return JEXL_EXPRESSION;
    }

    PrimaryKeyExpressionFactory getInstance() {
        return instance;
    }
}
