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

import org.apache.atlas.model.PList;
import org.apache.atlas.model.SearchFilter.SortType;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;


/**
 * An instance of an entity - like hive_table, hive_database.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasEntityId implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Status of the entity - can be active or deleted. Deleted entities are not removed from Atlas store.
     */
    public enum Status { STATUS_ACTIVE, STATUS_DELETED };

    private String typeName   = null;
    private String guid       = null;
    private Status status     = Status.STATUS_ACTIVE;
    private Long   version    = null;

    public AtlasEntityId() {
        this(null, null, Long.valueOf(0), Status.STATUS_ACTIVE);
    }

    public AtlasEntityId(String typeName) {
        this(typeName, null, Long.valueOf(0), Status.STATUS_ACTIVE);
    }


    public AtlasEntityId(String typeName,  String guid, long version) {
        this(typeName, guid, version, Status.STATUS_ACTIVE);
    }

    public AtlasEntityId(String typeName,  String guid, Long version, Status status) {
        setTypeName(typeName);
        setGuid(guid);
        setStatus(status);
        setVersion(version);
    }

    public AtlasEntityId(AtlasEntityId other) {
        if (other != null) {
            setTypeName(other.getTypeName());
            setGuid(other.getGuid());
            setStatus(other.getStatus());
            setVersion(other.getVersion());
        }
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(final String typeName) {
        this.typeName = typeName;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("AtlasEntity{");
        sb.append("typeName='").append(typeName).append('\'');
        sb.append("guid='").append(guid).append('\'');
        sb.append(", status=").append(status);
        sb.append(", version=").append(version);
        sb.append('}');

        return sb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }

        AtlasEntityId that = (AtlasEntityId) o;

        if ( typeName != null ? !typeName.equals(that.typeName) : that.typeName != null) {
            return false;
        }
        if (guid != null ? !guid.equals(that.guid) : that.guid != null) { return false; }
        if (status != null ? !status.equals(that.status) : that.status != null) { return false; }
        if (version != null ? !version.equals(that.version) : that.version != null) { return false; }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();

        result = 31 * result + (typeName != null ? typeName.hashCode(): 0);
        result = 31 * result + (guid != null ? guid.hashCode() : 0);
        result = 31 * result + (status != null ? status.hashCode() : 0);
        result = 31 * result + (version != null ? version.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    /**
     * REST serialization friendly list.
     */
    @JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    @XmlSeeAlso(AtlasEntityId.class)
    public static class AtlasEntityIds extends PList<AtlasEntityId> {
        private static final long serialVersionUID = 1L;

        public AtlasEntityIds() {
            super();
        }

        public AtlasEntityIds(List<AtlasEntityId> list) {
            super(list);
        }

        public AtlasEntityIds(List list, long startIndex, int pageSize, long totalCount,
            SortType sortType, String sortBy) {
            super(list, startIndex, pageSize, totalCount, sortType, sortBy);
        }
    }
}
