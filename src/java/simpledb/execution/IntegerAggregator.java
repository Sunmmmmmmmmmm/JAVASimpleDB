package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;

import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private abstract class AggHandler{
        HashMap<Field,Integer> aggResult;
        abstract void handle(Field field, IntField intField);
        public AggHandler(){
            aggResult = new HashMap<>();
        }
        public HashMap<Field,Integer> getAggResult(){
            return aggResult;
        }
    }


    private class CountHandler extends AggHandler{
        @Override
        void handle(Field field, IntField intField) {
            if(aggResult.containsKey(field)) {
                aggResult.put(field, aggResult.get(field)+1);
            }else {
                aggResult.put(field,1);
            }
        }
    }
    private class SumHandler extends AggHandler {

        @Override
        void handle(Field field, IntField intField) {
            if(aggResult.containsKey(field)) {
                aggResult.put(field,aggResult.get(field)+intField.getValue());
            }else {
                aggResult.put(field,intField.getValue());
            }
        }
    }

    private class MaxHandler extends AggHandler {

        @Override
        void handle(Field field, IntField intField) {
            if(aggResult.containsKey(field)) {
                aggResult.put(field,Math.max(aggResult.get(field),intField.getValue()));
            }else {
                aggResult.put(field,intField.getValue());
            }
        }
    }
    private class MinHandler extends AggHandler {

        @Override
        void handle(Field field, IntField intField) {
            if(aggResult.containsKey(field)) {
                aggResult.put(field,Math.min(aggResult.get(field),intField.getValue()));
            }else {
                aggResult.put(field,intField.getValue());
            }
        }
    }

    private class AvgHandler extends  AggHandler {
        HashMap<Field,Integer> sum;
        HashMap<Field,Integer> count;
        private void AggHandler() {
            sum = new HashMap<>();
            count = new HashMap<>();

        }

        @Override
        void handle(Field field, IntField intField) {
            if(sum.containsKey(field)) {
                sum.put(field,sum.get(field)+intField.getValue());
                count.put(field,count.get(field)+1);
            }else {
                sum.put(field,intField.getValue());
                count.put(field,1);
            }
            aggResult.put(field,sum.get(field)/count.get(field));
        }
    }

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private AggHandler aggHandler;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        switch (what) {
            case MIN:
                aggHandler = new MinHandler();
                break;
            case MAX:
                aggHandler = new MaxHandler();
                break;
            case SUM:
                aggHandler = new SumHandler();
                break;
            case COUNT:
                aggHandler = new CountHandler();
                break;
            case AVG:
                aggHandler = new AvgHandler();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported aggregation operator ");
        }

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField;
        IntField intField = (IntField) tup.getField(afield);
        if(afield == NO_GROUPING ) {
            gbField = null;
        }else {
            gbField = tup.getField(gbfield);
        }
        aggHandler.handle(gbField,intField);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        throw new
        UnsupportedOperationException("please implement me for lab2");
    }

}
