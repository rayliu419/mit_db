package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    ArrayList<TDItem> fieldDescriptions;

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        if (fieldDescriptions != null) {
            return fieldDescriptions.iterator();
        }
        return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        fieldDescriptions = new ArrayList();
        for (int i = 0; i < typeAr.length; i++) {
            fieldDescriptions.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        fieldDescriptions = new ArrayList();
        for (int i = 0; i < typeAr.length; i++) {
            fieldDescriptions.add(new TDItem(typeAr[i], ""));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return fieldDescriptions.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= fieldDescriptions.size()) {
            throw new NoSuchElementException();
        } else {
            return fieldDescriptions.get(i).fieldName;
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i >= fieldDescriptions.size()) {
            throw new NoSuchElementException();
        } else {
            return fieldDescriptions.get(i).fieldType;
        }
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < fieldDescriptions.size(); i++) {
            if (fieldDescriptions.get(i).fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        List<Integer> lengthArray = fieldDescriptions
                .stream()
                .map(e -> e.fieldType.getLen())
                .collect(Collectors.toList());
        return lengthArray.stream().mapToInt(Integer::intValue).sum();
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        Type[] td1TypeArr = new Type[td1.numFields()];
        String[] td1StringArr = new String[td1.numFields()];
        for (int i = 0; i < td1.numFields(); i++) {
            td1TypeArr[i] = td1.fieldDescriptions.get(i).fieldType;
            td1StringArr[i] = td1.fieldDescriptions.get(i).fieldName;
        }
        Type[] td2TypeArr = new Type[td2.numFields()];
        String[] td2StringArr = new String[td2.numFields()];
        for (int i = 0; i < td2.numFields(); i++) {
            td2TypeArr[i] = td2.fieldDescriptions.get(i).fieldType;
            td2StringArr[i] = td2.fieldDescriptions.get(i).fieldName;
        }
        Type[] mergeType = new Type[td1.numFields() + td2.numFields()];
        String[] mergeString = new String[td1.numFields() + td2.numFields()];
        System.arraycopy(td1TypeArr, 0, mergeType, 0, td1TypeArr.length);
        System.arraycopy(td2TypeArr, 0, mergeType, td1TypeArr.length, td2TypeArr.length);
        System.arraycopy(td1StringArr, 0, mergeString, 0, td1StringArr.length);
        System.arraycopy(td2StringArr, 0, mergeString, td1StringArr.length, td2StringArr.length);
        TupleDesc merge = new TupleDesc(mergeType, mergeString);
        return merge;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (this == o) return true;
        if (o instanceof TupleDesc) {
            TupleDesc other = (TupleDesc) o;
            if (this.numFields() != other.numFields()) {
                return false;
            }
            int totalFileds = this.numFields();
            for (int i = 0; i < totalFileds; i++) {
                if (this.fieldDescriptions.get(i).fieldType != other.fieldDescriptions.get(i).fieldType
                        || this.fieldDescriptions.get(i).fieldName != other.fieldDescriptions.get(i).fieldName) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String s = "";
        for (int i = 0; i < fieldDescriptions.size(); i++) {
            String cur = fieldDescriptions.get(i).fieldType.toString() + "(" +
                    fieldDescriptions.get(i).fieldName + ")";
            s += cur;
        }
        return s;
    }
}
