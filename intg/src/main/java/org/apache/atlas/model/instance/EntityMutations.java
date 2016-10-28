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

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.List;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class EntityMutations implements Serializable {

    private List<EntityMutation> entityMutations;

    public enum EntityOperation {
        CREATE_OR_UPDATE,
        PARTIAL_UPDATE,
        DELETE,
    }

    public static final class EntityMutation implements Serializable {
        private EntityOperation op;
        private AtlasEntity entity;

        public EntityMutation(EntityOperation op, AtlasEntity entity) {
            this.op = op;
            this.entity = entity;
        }

        public StringBuilder toString(StringBuilder sb) {
            if ( sb == null) {
                sb = new StringBuilder();
            }
            sb.append(op);
            sb.append(entity);
            return sb;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) { return true; }
            if (o == null || getClass() != o.getClass()) { return false; }

            EntityMutation that = (EntityMutation) o;

            if (op != null ? !op.equals(that.op) : that.op != null) { return false; }

            return true;
        }

        @Override
        public int hashCode() {
            int result = (op != null ? op.hashCode() : 0);
            result = 31 * result + (entity != null ? entity.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }
    }

    public EntityMutations(List<EntityMutation> entityMutations) {
        this.entityMutations = entityMutations;
    }

    public StringBuilder toString(StringBuilder sb) {
        if ( sb == null) {
            sb = new StringBuilder();
        }
        sb.append(entityMutations);
        return sb;
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        EntityMutations that = (EntityMutations) o;

        if (entityMutations != null ? !entityMutations.equals(that.entityMutations) : that.entityMutations != null) { return false; }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (entityMutations != null ? entityMutations.hashCode() : 0);
        return result;
    }
}


