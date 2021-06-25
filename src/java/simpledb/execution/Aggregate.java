package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 * 仅仅支持group by 一个字段，在另外一个字段aggreagate
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    // 要作用的元组的迭代器
    private OpIterator child;

    private int aggregateFieldIndex;

    private int groupByFieldIndex;

    private Aggregator.Op aop;

    private Aggregator aggregator;

    // 这个Iterator看起来是返回这个运算作用完以后的Iterator
    private OpIterator it;

    // 聚合结果的元组描述符
    private TupleDesc td;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.aggregateFieldIndex = afield;
        this.groupByFieldIndex = gfield;
        this.aop = aop;

        Type groupByFieldType = gfield == -1 ?
                null : this.child.getTupleDesc().getFieldType(this.groupByFieldIndex);

        if (this.child.getTupleDesc().getFieldType(this.aggregateFieldIndex) == (Type.STRING_TYPE)) {
            this.aggregator = new StringAggregator(this.groupByFieldIndex, groupByFieldType,
                    this.aggregateFieldIndex, this.aop);
        } else {
            this.aggregator = new IntegerAggregator(this.groupByFieldIndex, groupByFieldType,
                    this.aggregateFieldIndex, this.aop);
        }
        this.it = this.aggregator.iterator();
        // create tupleDesc for agg
        // 为aggregate的结果创建元组描述符
        List<Type> types = new ArrayList<>();
        List<String> names = new ArrayList<>();
        // group field
        if (groupByFieldType != null) {
            types.add(groupByFieldType);
            names.add(this.child.getTupleDesc().getFieldName(this.groupByFieldIndex));
        }
        types.add(this.child.getTupleDesc().getFieldType(this.aggregateFieldIndex));
        names.add(this.child.getTupleDesc().getFieldName(this.aggregateFieldIndex));
        if (aop.equals(Aggregator.Op.SUM_COUNT)) {
            // 同时计算sum和count，所有有三列
            types.add(Type.INT_TYPE);
            names.add("COUNT");
        }
        // aggregate的结果也是元组数组，也需要一个元组描述符
        this.td = new TupleDesc(types.toArray(new Type[types.size()]), names.toArray(new String[names.size()]));
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return groupByFieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        // 第一个字段就是group by
        return td.getFieldName(0);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return aggregateFieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        // 没有group by的字段
        if (this.groupByFieldIndex == -1)
            return this.td.getFieldName(0);
        else
            return this.td.getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        this.child.open();
        while (this.child.hasNext())
            this.aggregator.mergeTupleIntoGroup(this.child.next());
        this.it.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while (this.it.hasNext())
            return this.it.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        // 为什么child也要rewind?
        this.child.rewind();
        this.it.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
        this.it.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
        List<Type> types = new ArrayList<>();
        List<String> names = new ArrayList<>();
        Type gfieldtype = this.groupByFieldIndex == -1 ?
                null : this.child.getTupleDesc().getFieldType(this.groupByFieldIndex);
        // group field
        if (gfieldtype != null) {
            types.add(gfieldtype);
            names.add(this.child.getTupleDesc().getFieldName(this.groupByFieldIndex));
        }
        types.add(this.child.getTupleDesc().getFieldType(this.aggregateFieldIndex));
        names.add(this.child.getTupleDesc().getFieldName(this.aggregateFieldIndex));
        if (aop.equals(Aggregator.Op.SUM_COUNT)) {
            types.add(Type.INT_TYPE);
            names.add("COUNT");
        }
        assert (types.size() == names.size());
        this.td = new TupleDesc(types.toArray(new Type[types.size()]), names.toArray(new String[names.size()]));
    }

}
