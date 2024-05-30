package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {


    private File f;
    private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
//    public Page readPage(PageId pid) {
//        try {
//            RandomAccessFile raf = new RandomAccessFile(f, "r");
//            int pagesize = BufferPool.getPageSize();
//            byte[] b = new byte[pagesize];
//            raf.seek(pid.getPageNumber() * pagesize);
//            raf.read(b, 0, pagesize);
//            raf.close();
//            return new HeapPage((HeapPageId)pid, b);
//        } catch(Exception e) {
//            e.printStackTrace();
//            throw new IllegalArgumentException();
//        }
//    }
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        int pageNumber = pid.getPageNumber();
        //tableId和pageNumber 用于获取heapPageId
        int pageSize = Database.getBufferPool().getPageSize();
        long offset = pageNumber * pageSize;
        byte[] data = new byte[pageSize];
        RandomAccessFile rfile = null;
        try{
            rfile = new RandomAccessFile(f,"r");
            rfile.seek(offset);
            rfile.read(data);
            HeapPageId heapPageId = new HeapPageId(tableId,pageNumber);
            HeapPage heapPage = new HeapPage(heapPageId,data);
            return heapPage;

        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("HeapFile: readPage: file not found");
        } catch (IOException e) {
            throw new IllegalArgumentException(String.format("HeapFile: readPage: file with offset %d not found",offset));
        } finally {
            try {
                rfile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId id = page.getId();
        int pageNumber = id.getPageNumber();
        int pageSize = Database.getBufferPool().getPageSize();
        byte[] pageData = page.getPageData();

        RandomAccessFile randomAccessFile = new RandomAccessFile(f, "rws");
        randomAccessFile.skipBytes(pageNumber*pageSize);
        randomAccessFile.write(pageData);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
//        System.out.println(f.length());
//        System.out.println(Database.getBufferPool().getPageSize());

        return (int)(f.length()/ Database.getBufferPool().getPageSize() );
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> pageArr = new ArrayList<>();
        for(int pageNo =0;pageNo<numPages();pageNo++) {
            HeapPageId heapPageId = new HeapPageId(getId(), pageNo);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            if(page.getNumEmptySlots()!=0) {
                page.insertTuple(t);
                pageArr.add(page);
                return pageArr;
            }else {
                Database.getBufferPool().unsafeReleasePage(tid,heapPageId);
                continue;
            }
        }

        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(f, true));
        byte[] emptyPageData = HeapPage.createEmptyPageData();
        bufferedOutputStream.write(emptyPageData);
        bufferedOutputStream.close();

        HeapPageId heapPageId = new HeapPageId(getId(), numPages() - 1);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
        page.insertTuple(t);
        pageArr.add(page);
        return pageArr;

    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pageList = new ArrayList<>();
        PageId pageId = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        pageList.add(page);
        return pageList;
    }

//    public DbFileIterator iterator(TransactionId tranid) {
//        return new DbFileIterator(){
//            private TransactionId tid = tranid;
//            private int pageno = 0;
//            private HeapPage p;
//            private Iterator<Tuple> pgit;
//
//            public void open() throws DbException, TransactionAbortedException {
//                pageno = 0;
//                p = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageno), Permissions.READ_ONLY);
//                pgit = p.iterator();
//            }
//
//            public boolean hasNext() throws DbException, TransactionAbortedException {
//                if(tid == null || pageno == numPages()) return false;
//                while(!pgit.hasNext()){
//                    pageno += 1;
//                    if(pageno == numPages()) return false;
//                    p = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageno), Permissions.READ_ONLY);
//                    pgit = p.iterator();
//                }
//                return true;
//            }
//
//            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
//                if(!hasNext())
//                    throw new NoSuchElementException();
//                return pgit.next();
//            }
//
//            public void rewind() throws DbException, TransactionAbortedException {
//                pageno = 0;
//                p = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageno), Permissions.READ_ONLY);
//                pgit = p.iterator();
//            }
//
//            public void close() {
//                tid = null;
//                pageno = 0;
//                p = null;
//                pgit = null;
//            }
//        };
//    }


    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }
    private class HeapFileIterator implements DbFileIterator{
        private final TransactionId tid;
        private Iterator<Tuple> tupsIterator;
        private final int tableId;
        private final int numPages;
        private int pageNo;


        public HeapFileIterator(TransactionId transactionId) {
            this.tid = transactionId;
            tableId = getId();
            numPages = numPages();
        }


        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageNo = 0;
            tupsIterator = getTuplesIterator(pageNo);
        }

        private Iterator<Tuple> getTuplesIterator(int pageNumber) throws DbException, TransactionAbortedException {
            if(pageNumber>=0 && pageNumber<=numPages){
                HeapPageId heapPageId = new HeapPageId(tableId,pageNumber);
                HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
                return heapPage.iterator();
            } else {
                throw new DbException(String.format("heapfile %d does not contain page %d!",tableId, pageNumber));
            }

        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(tupsIterator == null){
                return false;
            }
            if(!tupsIterator.hasNext()){
                if(pageNo < (numPages-1)){
                    pageNo++;
                    tupsIterator = getTuplesIterator(pageNo);
                    return tupsIterator.hasNext();
                }else {
                    return false;
                }
            } else {
                return true;
            }

        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(tupsIterator==null || !tupsIterator.hasNext()){
                throw new NoSuchElementException("This is the last element");
            }
            return tupsIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            tupsIterator = null;
        }
    }


}

