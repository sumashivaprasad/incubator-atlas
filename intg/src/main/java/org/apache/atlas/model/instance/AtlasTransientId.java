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
package org.apache.atlas.model.instance;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.PList;
import org.apache.atlas.model.SearchFilter.SortType;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Reference to an object-instance of an Atlas type - like entity.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasTransientId implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String KEY_TYPENAME = "typeName";
    public static final String KEY_ID     = "id";

    private String typeName;
    private String id;

    private static AtomicLong s_nextId = new AtomicLong(System.nanoTime());

    public AtlasTransientId() {
        this(null, String.valueOf(nextNegativeLong()));
    }

    public AtlasTransientId(String typeName) {
        this(typeName, String.valueOf(nextNegativeLong()));
    }

    public AtlasTransientId(String typeName, String id)  {
        setTypeName(typeName);
        if (StringUtils.isEmpty(id)) {
            id = String.valueOf(nextNegativeLong());
        }
        setId(id);
    }

    public AtlasTransientId(AtlasTransientId other) {
        if (other != null) {
            setTypeName(other.getTypeName());
            setId(other.getId());
        }
    }

    public AtlasTransientId(Map objIdMap) {
        if (objIdMap != null) {
            Object t = objIdMap.get(KEY_TYPENAME);
            Object g = objIdMap.get(KEY_ID);

            if (t != null) {
                setTypeName(t.toString());
            }

            if (g != null) {
                setId(g.toString());
            }
        }
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("AtlasTransientId{");
        sb.append("typeName='").append(typeName).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append('}');

        return sb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        AtlasTransientId that = (AtlasTransientId) o;

        if (typeName != null ? !typeName.equals(that.typeName) : that.typeName != null) { return false; }
        if (id != null ? !id.equals(that.id) : that.id != null) { return false; }

        return true;
    }

    @Override
    public int hashCode() {
        int result = typeName != null ? typeName.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    private static long nextNegativeLong() {
        long ret = s_nextId.getAndDecrement();

        if (ret > 0) {
            ret *= -1;
        } else if (ret == 0) {
            ret = Long.MIN_VALUE;
        }

        return ret;
    }

    public boolean validate(String id) {
        try {
            long l = Long.parseLong(id);
            return l < 0;
        } catch (NumberFormatException ne) {
            return false;
        }
    }


    /**
     * REST serialization friendly list.
     */
    @JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    @XmlSeeAlso(AtlasTransientId.class)
    public static class AtlasTransientIds extends PList<AtlasTransientId> {
        private static final long serialVersionUID = 1L;

        public AtlasTransientIds() {
            super();
        }

        public AtlasTransientIds(List<AtlasTransientId> list) {
            super(list);
        }

        public AtlasTransientIds(List list, long startIndex, int pageSize, long totalCount,
                              SortType sortType, String sortBy) {
            super(list, startIndex, pageSize, totalCount, sortType, sortBy);
        }
    }
}
