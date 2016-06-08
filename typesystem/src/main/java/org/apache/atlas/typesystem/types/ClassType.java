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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.exception.ConstraintViolationException;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.ReferenceableInstance;
import org.apache.atlas.typesystem.persistence.StructInstance;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClassType extends HierarchicalType<ClassType, IReferenceableInstance>
        implements IConstructableType<IReferenceableInstance, ITypedReferenceableInstance> {

    public static final String TRAIT_NAME_SEP = "::";

    public final Map<AttributeInfo, List<String>> infoToNameMap;
    public final PrimaryKeyConstraint primaryKey;
    public final List<AttributeInfo> primaryKeyAttributes;

    ClassType(TypeSystem typeSystem, String name, String description, ImmutableSet<String> superTypes, PrimaryKeyConstraint pkc, int numFields) {
        super(typeSystem, ClassType.class, name, description, superTypes, pkc != null && pkc.isVisible() ? numFields + 1 : numFields);
        infoToNameMap = null;
        primaryKey = pkc;
        primaryKeyAttributes = null;
    }

    ClassType(TypeSystem typeSystem, String name, String description, ImmutableSet<String> superTypes, AttributeInfo... fields)
    throws AtlasException {
        super(typeSystem, ClassType.class, name, description, superTypes, fields);
        infoToNameMap = TypeUtils.buildAttrInfoToNameMap(fieldMapping);
        primaryKey = null;
        primaryKeyAttributes = null;
    }

    ClassType(TypeSystem typeSystem, String name, String description, ImmutableSet<String> superTypes, AttributeInfo[] fields, PrimaryKeyConstraint pkc)
        throws AtlasException {
        super(typeSystem, ClassType.class, name, description, superTypes, validateAndAddPKAttribute(typeSystem, pkc, fields));
        infoToNameMap = TypeUtils.buildAttrInfoToNameMap(fieldMapping);
        this.primaryKey = pkc;
        this.primaryKeyAttributes = hasPrimaryKey() ? new ArrayList<AttributeInfo>(getPrimaryKey().columns().size()) {{
            for (String col : getPrimaryKey().columns()) {
                add(fieldMapping.fields.get(col));
            }
        }} : null;
    }

    private static AttributeInfo[] validateAndAddPKAttribute(TypeSystem ts, PrimaryKeyConstraint pkc, AttributeInfo[] fields) throws AtlasException {

        boolean containsPrimaryKey = false;
        for (AttributeInfo field : fields) {

            //TODO - Enable this for primary keys
//            if (validateMultiplicity(pkc, field) ) {
//                throw new ConstraintViolationException("Primary key column '" + field.name + "' should have 'required(Multiplicity.REQUIRED) set");
//            }

            if ( field.name.equalsIgnoreCase(PrimaryKeyConstraint.PK_ATTR_NAME)) {
               containsPrimaryKey = true;
               break;
            }
        }

        if ( !containsPrimaryKey ) {
            final boolean addPKAttr = pkc != null && pkc.isVisible();
            AttributeInfo[] result = addPKAttr ?
                Arrays.copyOf(fields, fields.length + 1) :
                fields;

            if (addPKAttr) {
                AttributeInfo pkAttr = new AttributeInfo(ts, new AttributeDefinition(PrimaryKeyConstraint.PK_ATTR_NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null), null);
                result[result.length - 1] = pkAttr;
            }
            return result;
        }

        return fields;
    }

    private static boolean validateMultiplicity(final PrimaryKeyConstraint pkc, final AttributeInfo field) {
        if ( pkc.columns().contains(field.name) ) {
            return field.multiplicity.nullAllowed();
        }
        return false;
    }

    @Override
    public DataTypes.TypeCategory getTypeCategory() {
        return DataTypes.TypeCategory.CLASS;
    }

    public void validateId(Id id) throws AtlasException {
        if (id != null) {
            ClassType cType = typeSystem.getDataType(ClassType.class, id.typeName);
            if (isSubType(cType.getName())) {
                return;
            }
            throw new AtlasException(String.format("Id %s is not valid for class %s", id, getName()));
        }
    }

    protected Id getId(Object val) throws AtlasException {
        if (val instanceof Referenceable) {
            return ((Referenceable) val).getId();
        }
        throw new AtlasException(String.format("Cannot get id from class %s", val.getClass()));
    }


    @Override
    public ITypedReferenceableInstance convert(Object val, Multiplicity m) throws AtlasException {
        if (val != null) {
            if (val instanceof ITypedReferenceableInstance) {
                ITypedReferenceableInstance tr = (ITypedReferenceableInstance) val;
                if (!tr.getTypeName().equals(getName())) {
                     /*
                     * If val is a subType instance; invoke convert on it.
                     */
                    ClassType valType = typeSystem.getDataType(superTypeClass, tr.getTypeName());
                    if (valType.superTypePaths.containsKey(name)) {
                        return valType.convert(val, m);
                    }
                    throw new ValueConversionException(this, val);
                }
                return tr;
            } else if (val instanceof Struct) {
                Struct s = (Struct) val;
                Referenceable r = null;
                Id id = null;

                if (!s.typeName.equals(getName())) {
                    /*
                     * If val is a subType instance; invoke convert on it.
                     */
                    ClassType valType = typeSystem.getDataType(superTypeClass, s.typeName);
                    if (valType.superTypePaths.containsKey(name)) {
                        return valType.convert(s, m);
                    }
                    throw new ValueConversionException(this, val);
                }

                if (val instanceof Referenceable) {
                    r = (Referenceable) val;
                    id = r.getId();
                }

                ITypedReferenceableInstance tr =
                        r != null ? createInstanceWithTraits(id, r, r.getTraits().toArray(new String[0])) :
                                createInstance(id);

                if (id != null && id.isAssigned()) {
                    return tr;
                }

                for (Map.Entry<String, AttributeInfo> e : fieldMapping.fields.entrySet()) {
                    String attrKey = e.getKey();
                    AttributeInfo i = e.getValue();
                    Object aVal = s.get(attrKey);
                    if (aVal != null && i.dataType().getTypeCategory() == DataTypes.TypeCategory.CLASS) {
                        if (!i.isComposite) {
                            aVal = ((IReferenceableInstance) aVal).getId();
                        }
                    }

                    try {
                        tr.set(attrKey, aVal);
                    } catch (ValueConversionException ve) {
                        throw new ValueConversionException(this, val, ve);
                    }
                }

                return tr;
            } else if (val instanceof ReferenceableInstance) {
                validateId(((ReferenceableInstance) val).getId());
                return (ReferenceableInstance) val;
            } else {
                throw new ValueConversionException(this, val, "value's class is " + val.getClass().getName());
            }
        }
        if (!m.nullAllowed()) {
            throw new ValueConversionException.NullConversionException(m);
        }
        return null;
    }

    @Override
    public ITypedReferenceableInstance createInstance() throws AtlasException {
        return createInstance((String[]) null);
    }

    public ITypedReferenceableInstance createInstance(String... traitNames) throws AtlasException {
        return createInstance(null, traitNames);
    }

    public ITypedReferenceableInstance createInstance(Id id, String... traitNames) throws AtlasException {
        return createInstanceWithTraits(id, null, traitNames);
    }

    public ITypedReferenceableInstance createInstanceWithTraits(Id id, Referenceable r, String... traitNames)
    throws AtlasException {

        ImmutableMap.Builder<String, ITypedStruct> b = new ImmutableBiMap.Builder<String, ITypedStruct>();
        if (traitNames != null) {
            for (String t : traitNames) {
                TraitType tType = typeSystem.getDataType(TraitType.class, t);
                IStruct iTraitObject = r == null ? null : r.getTrait(t);
                ITypedStruct trait = iTraitObject == null ? tType.createInstance() :
                        tType.convert(iTraitObject, Multiplicity.REQUIRED);
                b.put(t, trait);
            }
        }

        int numStrings = fieldMapping.numStrings;
        if ( hasPrimaryKey() && primaryKey.isVisible()) {
            numStrings += 1;
        }

        return new ReferenceableInstance(id == null ? new Id(getName()) : id, getName(), fieldMapping,
                new boolean[fieldMapping.fields.size()],
                fieldMapping.numBools == 0 ? null : new boolean[fieldMapping.numBools],
                fieldMapping.numBytes == 0 ? null : new byte[fieldMapping.numBytes],
                fieldMapping.numShorts == 0 ? null : new short[fieldMapping.numShorts],
                fieldMapping.numInts == 0 ? null : new int[fieldMapping.numInts],
                fieldMapping.numLongs == 0 ? null : new long[fieldMapping.numLongs],
                fieldMapping.numFloats == 0 ? null : new float[fieldMapping.numFloats],
                fieldMapping.numDoubles == 0 ? null : new double[fieldMapping.numDoubles],
                fieldMapping.numBigDecimals == 0 ? null : new BigDecimal[fieldMapping.numBigDecimals],
                fieldMapping.numBigInts == 0 ? null : new BigInteger[fieldMapping.numBigInts],
                fieldMapping.numDates == 0 ? null : new Date[fieldMapping.numDates],
                numStrings == 0 ? null : new String[numStrings],
                fieldMapping.numArrays == 0 ? null : new ImmutableList[fieldMapping.numArrays],
                fieldMapping.numMaps == 0 ? null : new ImmutableMap[fieldMapping.numMaps],
                fieldMapping.numStructs == 0 ? null : new StructInstance[fieldMapping.numStructs],
                fieldMapping.numReferenceables == 0 ? null : new ReferenceableInstance[fieldMapping.numReferenceables],
                fieldMapping.numReferenceables == 0 ? null : new Id[fieldMapping.numReferenceables],
                b.build());
    }

    @Override
    public void output(IReferenceableInstance s, Appendable buf, String prefix, Set<IReferenceableInstance> inProcess) throws AtlasException {
        fieldMapping.output(s, buf, prefix, inProcess);
    }

    @Override
    public List<String> getNames(AttributeInfo info) {
        return infoToNameMap.get(info);
    }

    @Override
    public void updateSignatureHash(MessageDigest digester, Object val) throws AtlasException {
        if( !(val instanceof  ITypedReferenceableInstance)) {
            throw new IllegalArgumentException("Unexpected value type " + val.getClass().getSimpleName() + ". Expected instance of ITypedStruct");
        }
        digester.update(getName().getBytes(Charset.forName("UTF-8")));

        if(fieldMapping.fields != null && val != null) {
            IReferenceableInstance typedValue = (IReferenceableInstance) val;
            if(fieldMapping.fields.values() != null) {
                for (AttributeInfo aInfo : fieldMapping.fields.values()) {
                    Object attrVal = typedValue.get(aInfo.name);
                    if (attrVal != null) {
                        aInfo.dataType().updateSignatureHash(digester, attrVal);
                    }
                }
            }
        }
    }

    public PrimaryKeyConstraint getPrimaryKey() {
        return primaryKey;
    }

    public boolean hasPrimaryKey() {
        return primaryKey != null;
    }

    public boolean isPrimaryKeyAttribute(String attrName) {
        if (hasPrimaryKey()) {
            return getPrimaryKey().columns().contains(attrName);
        }
        return false;
    }

    public List<AttributeInfo> getPrimaryKeyAttrs( ) {
        return primaryKeyAttributes;
    }
}