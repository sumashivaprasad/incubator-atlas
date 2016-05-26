/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.typesystem.types;


import com.google.common.base.Joiner;
import org.apache.atlas.AtlasException;
import org.apache.atlas.utils.ParamChecker;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JxltEngine;
import org.apache.commons.jexl3.MapContext;
import scala.actors.threadpool.Arrays;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.jexl3.JexlContext;

/**
 * A primary key constraint that can be defined on hierarchical types like class, trait.
 * Supports a set of unique columns i.e a composite primary key.
 * The specified primary key columns should already be part of the attribute definition of the class
 * and should be required attributes
 */
//TODO :Extending this from an interface is causing issues during deserialization from json using json4s.
public class PrimaryKeyConstraint {
//    implements javax.persistence.UniqueConstraint {

    private final List<String> uniqueColumns;
    private final String displayFormat;
    private final boolean isVisible;

    public static final String PK_ATTR_NAME = "qualifiedName";

    PrimaryKeyConstraint(List<String> uniqueColumns, boolean isVisible, String displayFormat) {
        this.uniqueColumns = uniqueColumns;
        this.displayFormat = displayFormat;
        this.isVisible = isVisible;
    }

    public static PrimaryKeyConstraint of(Iterable<String> uniqueColumns, boolean isVisible, String displayFormat) {
        List<String> temp = new ArrayList<>();
        final Iterator<String> iter = uniqueColumns.iterator();
        while (iter.hasNext()) {
            temp.add(iter.next());
        }
        return new PrimaryKeyConstraint(temp, isVisible, displayFormat);
    }

    public static PrimaryKeyConstraint of(List<String> uniqueColumns, boolean isVisible, String displayFormat) {
        return new PrimaryKeyConstraint(uniqueColumns, isVisible, displayFormat);
    }

    public static PrimaryKeyConstraint of(final String... uniqueColumns) {
        ParamChecker.notNull(uniqueColumns, "Primary key columns");
        return new PrimaryKeyConstraint(Arrays.asList(uniqueColumns), true, null);
    }

    public List<String> columns() {
        return uniqueColumns;
    }

    public boolean isVisible() {
        return isVisible;
    }

    public String displayFormat() {
        return displayFormat;
    }

    public String displayValue(Map<String, String> pkValues) {
        if (displayFormat() == null) {
            return Joiner.on(":").join(pkValues.values());
        } else {
            String exprString = displayFormat();
            JexlEngine jexl = new JexlBuilder().create();
            // Create an expression
            JxltEngine jxlt = jexl.createJxltEngine();
            JxltEngine.Expression expr = jxlt.createExpression(exprString);

            // Create a context and add data
            JexlContext jc = new MapContext();
            for (String key : pkValues.keySet()) {
                jc.set(key, pkValues.get(key));
            }

            // Now evaluate the expression, getting the result
            return (String) expr.evaluate(jc);
        }
    }

    public String attributeName() {
        return PK_ATTR_NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PrimaryKeyConstraint that = (PrimaryKeyConstraint) o;

        if (!uniqueColumns.equals(that.columns())) {
            return false;
        }

        if (displayFormat != null && !displayFormat.equals(that.displayFormat())) {
            return false;
        }

        return isVisible == that.isVisible();
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + uniqueColumns.hashCode();
        result = 31 * result + ((isVisible == true) ? 1 : 0);
        result = 31 * result + ((displayFormat != null ) ? displayFormat.hashCode() : 0);
        return result;
    }
}
