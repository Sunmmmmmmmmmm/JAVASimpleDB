package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.Arrays;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    TransactionId transactionId;
    OpIterator opIterator;
    int tableId;
    boolean isInserted;
    TupleDesc tupleDesc;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        if(!Database.getCatalog().getTupleDesc(tableId).equals(child.getTupleDesc())){
            throw new DbException("TupleDesc does not match!");
        }

        transactionId = t;
        opIterator = child;
        this.tableId = tableId;
        isInserted = false;
        Type[] types = {Type.INT_TYPE};
//        String[] strings = {"numbers of instered tuples"};
        this.tupleDesc = new TupleDesc(types);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        opIterator.open();
        super.open();
    }

    public void close() {
        // some code goes here
        opIterator.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!isInserted) {
            isInserted = true;
            int count = 0;
            while(opIterator.hasNext()) {
                Tuple next = opIterator.next();
                try {
                    Database.getBufferPool().insertTuple(transactionId,tableId,next);
                    count++;

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0,new IntField(count));
            return tuple;
        }else {
            return null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{opIterator};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        opIterator = children[0];
    }
}
