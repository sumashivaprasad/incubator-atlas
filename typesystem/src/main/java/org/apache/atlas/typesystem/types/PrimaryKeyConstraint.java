package org.apache.atlas.typesystem.types;


import org.apache.atlas.utils.ParamChecker;

import java.lang.annotation.Annotation;

public class PrimaryKeyConstraint implements javax.persistence.UniqueConstraint {

    private final String[] uniqueColumns;

    PrimaryKeyConstraint(String[] uniqueColumns) {
        this.uniqueColumns  = uniqueColumns;
    }

    public static PrimaryKeyConstraint of(String... uniqueColumns) {
        ParamChecker.notNull(uniqueColumns, "Primary key columns");
        return new PrimaryKeyConstraint(uniqueColumns);
    }

    @Override
    public String[] columnNames() {
        return uniqueColumns;
    }

    @Override
    public Class<? extends Annotation> annotationType() {
        return PrimaryKeyConstraint.class;
    }
}
