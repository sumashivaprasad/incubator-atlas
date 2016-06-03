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

import com.google.common.base.Joiner;
import org.apache.atlas.typesystem.types.EvaluationException;
import org.apache.atlas.typesystem.types.PrimaryKeyExpression;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JxltEngine;
import org.apache.commons.jexl3.MapContext;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JEXLExpression implements PrimaryKeyExpression {

    private JexlEngine jexl = new JexlBuilder().create();
    private JxltEngine jxlt = jexl.createJxltEngine();

    @Override
    public String evaluate(final String format, final LinkedHashMap<String, String> variables) throws EvaluationException {
        String[] exprColumns = new String[variables.size()];
        int i = 0;
        for (String attrName : variables.keySet()) {
            exprColumns[i++] = "${" + attrName + '}';
        }

        String expression = String.format(format, exprColumns);

        // Create an expression
        JxltEngine.Expression expr = jxlt.createExpression(expression);

        // Create a context and add data
        JexlContext jc = new MapContext();
        for (String key : variables.keySet()) {
            jc.set(key, variables.get(key));
        }

        // Now evaluate the expression, getting the result
        return (String) expr.evaluate(jc);
    }


    @Override
    public String type() {
        return "jexl";
    }
}
