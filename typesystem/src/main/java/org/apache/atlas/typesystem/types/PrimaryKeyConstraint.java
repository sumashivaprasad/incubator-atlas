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


import org.apache.atlas.utils.ParamChecker;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class PrimaryKeyConstraint  {
//    implements javax.persistence.UniqueConstraint {

    private final String[] uniqueColumns;

    PrimaryKeyConstraint(String[] uniqueColumns) {
        this.uniqueColumns  = uniqueColumns;
    }

    PrimaryKeyConstraint(Collection<String> uniqueColumns) {
        String[] uniqueArr = new String[uniqueColumns.size()];
        this.uniqueColumns  = uniqueColumns.toArray(uniqueArr);
    }

    public static PrimaryKeyConstraint of(Iterable<String> uniqueColumns) {
        List<String> temp = new ArrayList<>();
        final Iterator<String> iter = uniqueColumns.iterator();
        while (iter.hasNext()) {
            temp.add(iter.next());
        }
        String[] uniqueArr = new String[temp.size()];
        return new PrimaryKeyConstraint(temp.toArray(uniqueArr));
    }

    public static PrimaryKeyConstraint of(String... uniqueColumns) {
        ParamChecker.notNull(uniqueColumns, "Primary key columns");
        return new PrimaryKeyConstraint(uniqueColumns);
    }
//
//    @Override
    public String[] columnNames() {
        return uniqueColumns;
    }

//    @Override
//    public boolean equals(Object o) {
//        if (this == o) {
//            return true;
//        }
//        if (o == null || getClass() != o.getClass()) {
//            return false;
//        }
//        if (!super.equals(o)) {
//            return false;
//        }
//
//        PrimaryKeyConstraint that = (PrimaryKeyConstraint) o;
//
//        if (!uniqueColumns.equals(that.uniqueColumns)) {
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public int hashCode() {
//        int result = super.hashCode();
//        result = 31 * result + uniqueColumns.hashCode();
//        return result;
//    }

//    @Override
//    public Class<? extends Annotation> annotationType() {
//        return PrimaryKeyConstraint.class;
//    }
}
