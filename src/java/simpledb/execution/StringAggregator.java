package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private abstract class AggHandler{
        HashMap<Field, Integer> aggResult;
        abstract void handle(Field field, StringField intField);
        public AggHandler(){
            aggResult = new HashMap<>();
        }
        public HashMap<Field, Integer> getAggResult(){
            return aggResult;
        }
    }


    private class CountHandler extends AggHandler {
        @Override
        void handle(Field field, StringField intField) {
            if(aggResult.containsKey(field)) {
                aggResult.put(field, aggResult.get(field)+1);
            }else {
                aggResult.put(field,1);
            }
        }
    }

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private AggHandler aggHandler;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        switch (what) {
            case COUNT:
                aggHandler = new CountHandler();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported aggregation operator ");

        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField;
        StringField strField = (StringField) tup.getField(afield);
        if(gbfield == NO_GROUPING ) {
            gbField = null;
        }else {
            gbField = tup.getField(gbfield);
        }
        aggHandler.handle(gbField,strField);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        HashMap<Field, Integer> aggResult = aggHandler.getAggResult();
        Type[] fieldTypes;
        String[] fieldNames;
        TupleDesc tupleDesc;
        List<Tuple> tuples = new ArrayList<>();
        if(gbfield == NO_GROUPING) {
            fieldTypes = new Type[]{Type.INT_TYPE};
            fieldNames = new String[]{"aggregateValue"};
            tupleDesc = new TupleDesc(fieldTypes,fieldNames);
            Tuple tuple = new Tuple(tupleDesc);
            IntField intField = new IntField(aggResult.get(null));
            tuple.setField(0,intField);
            tuples.add(tuple);
        }else {
            fieldTypes = new Type[]{gbfieldtype,Type.INT_TYPE};
            fieldNames = new String[]{"groupByValue" , "aggregateValue"};
            tupleDesc = new TupleDesc(fieldTypes,fieldNames);
            for(Field field : aggResult.keySet()){
                Tuple tuple = new Tuple(tupleDesc);
                if(gbfieldtype == Type.INT_TYPE){
                    IntField gbField = (IntField)field;
                    tuple.setField(0,gbField);
                } else {
                    StringField gbField = (StringField) field;
                    tuple.setField(0,gbField);
                }

                IntField resultField = new IntField(aggResult.get(field));
                tuple.setField(1,resultField);
                tuples.add(tuple);
            }
        }
        return new TupleIterator(tupleDesc,tuples);
    }

}
