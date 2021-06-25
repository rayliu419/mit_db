package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupByFieldIndex;

    private Type groupByFieldType;

    private int aggregateFieldIndex;

    private Op what;

    private Map<Field, Integer> groupMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (!what.equals(Op.COUNT))
            throw new IllegalArgumentException("Only COUNT is supported for String fields!");
        this.groupByFieldIndex = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateFieldIndex = afield;
        this.what = what;
        this.groupMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        StringField afield = (StringField) tup.getField(this.aggregateFieldIndex);
        Field gbfield = this.groupByFieldIndex == NO_GROUPING ? null : tup.getField(this.groupByFieldIndex);
        // 聚合的数值
        String newValue = afield.getValue();
        if (gbfield != null && gbfield.getType() != this.groupByFieldType) {
            throw new IllegalArgumentException("Given tuple has wrong type");
        }
        // 对于string的聚合，其实就是string的count
        if (!this.groupMap.containsKey(gbfield))
            this.groupMap.put(gbfield, 1);
        else
            this.groupMap.put(gbfield, this.groupMap.get(gbfield) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new AggregateIterator(this.groupMap, this.groupByFieldType);
    }

}
