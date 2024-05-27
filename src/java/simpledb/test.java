package simpledb;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.SeqScan;
import simpledb.storage.HeapFile;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;

public class test {
    public static void main(String[] args) {
        Type[] typeAr = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String[] fieldAr = new String[]{"filed0", "field1", "field2"};
        TupleDesc tupleDesc = new TupleDesc(typeAr, fieldAr);
        File file = new File("D:\\1_Fucking_Java\\project\\simple-db-hw-2021-master\\src\\java\\simpledb\\some_data_file.txt");
        //System.out.println(file.length());
        HeapFile heapFile = new HeapFile(file, tupleDesc);
        Database.getCatalog().addTable(heapFile, "testTable", "filed0");

        TransactionId tid = new TransactionId();
        int tableId = heapFile.getId();
        SeqScan seqScan = new SeqScan(tid, tableId);
        try {
            seqScan.open();
            while (seqScan.hasNext()) {
                Tuple tuple = seqScan.next();
                System.out.println(tuple);
            }
            Database.getBufferPool().transactionComplete(tid);
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (DbException e) {
            e.printStackTrace();
        } finally {
            seqScan.close();
        }

    }
}
