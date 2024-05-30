package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;



/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;

    private HashMap<PageId,LinkedNode> bufferPool;
    private LinkedNode head, tail;
    private LockManager lockManager;
    public class LinkedNode {
        PageId pageId;
        Page page;
        LinkedNode prev;
        LinkedNode next;
        public LinkedNode() {}
        public LinkedNode(PageId _pageId, Page _page) {pageId = _pageId; page = _page;}
    }
    private void addToHead(LinkedNode node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    private void removeNode(LinkedNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private void moveToHead(LinkedNode node) {
        removeNode(node);
        addToHead(node);
    }

    private LinkedNode removeTail() {
        LinkedNode res = tail.prev;
        removeNode(res);
        return res;
    }

    private class PageLock{
        public static final int SHARE = 0;
        public static final int EXCLUSIVE = 1;
        private TransactionId tid;
        private int type;

        public PageLock(TransactionId tid, int type){
            this.tid =  tid;
            this.type = type;
        }

        public TransactionId getTid() {
            return tid;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }
    }

    private class LockManager{
        public ConcurrentHashMap<PageId,ConcurrentHashMap<TransactionId,PageLock>> lockMap;
        public LockManager(){
            lockMap = new ConcurrentHashMap<>();
        }

        /**
         * 获得锁逻辑
         * @param pageId pageID
         * @param tid 事务id
         * @param requiredType 获得锁类型
         * @return
         * @throws TransactionAbortedException
         * @throws InterruptedException
         */
        public synchronized boolean acquireLock(PageId pageId, TransactionId tid, int requiredType) throws TransactionAbortedException, InterruptedException {
            final String lockType = requiredType == 0 ? "read lock" : "write lock";
            final String thread = Thread.currentThread().getName();
            if(lockMap.get(pageId) == null){
                PageLock pageLock = new PageLock(tid,requiredType);
                ConcurrentHashMap<TransactionId,PageLock> pageLocks = new ConcurrentHashMap<>();
                pageLocks.put(tid,pageLock);
                lockMap.put(pageId,pageLocks);
                //System.out.println(thread + ": the " + pageId + " have no lock, transaction" + tid + " require " + lockType + ", accept");
                return true;
            }
            ConcurrentHashMap<TransactionId,PageLock> pageLocks = lockMap.get(pageId);

            if(pageLocks.get(tid) == null){
                // tid没有该page上的锁
                if(pageLocks.size() > 1){
                    //page 上有其他事务的读锁
                    if (requiredType == PageLock.SHARE){
                        //tid 请求读锁
                        PageLock pageLock = new PageLock(tid,PageLock.SHARE);
                        pageLocks.put(tid,pageLock);
                        lockMap.put(pageId,pageLocks);
                        //System.out.println(thread + ": the " + pageId + " have many read locks, transaction" + tid + " require " + lockType + ", accept and add a new read lock");
                        return true;
                    }
                    if (requiredType == PageLock.EXCLUSIVE){
                        // tid 需要获取写锁
                        wait(20);
                        System.out.println(thread + ": the " + pageId + " have lock with diff txid, transaction" + tid + " require write lock, await...");
                        return false;
                    }
                }
                if (pageLocks.size() == 1){
                    //page 上有一个其他事务的锁  可能是读锁，也可能是写锁
                    PageLock curLock = null;
                    for (PageLock lock : pageLocks.values()){
                        curLock = lock;
                    }
                    if (curLock.getType() == PageLock.SHARE){
                        //如果是读锁
                        if (requiredType == PageLock.SHARE){
                            // tid 需要获取的是读锁
                            PageLock pageLock = new PageLock(tid,PageLock.SHARE);
                            pageLocks.put(tid,pageLock);
                            lockMap.put(pageId,pageLocks);
                            //System.out.println(thread + ": the " + pageId + " have one read lock with diff txid, transaction" + tid + " require read lock, accept and add a new read lock");
                            return true;
                        }
                        if (requiredType == PageLock.EXCLUSIVE){
                            // tid 需要获取写锁
                            wait(10);
                            System.out.println(thread + ": the " + pageId + " have lock with diff txid, transaction" + tid + " require write lock, await...");
                            return false;
                        }
                    }
                    if (curLock.getType() == PageLock.EXCLUSIVE){
                        // 如果是写锁
                        wait(10);
                        System.out.println(thread + ": the " + pageId + " have one write lock with diff txid, transaction" + tid + " require read lock, await...");
                        return false;
                    }
                }

            }
            if (pageLocks.get(tid) != null){
                // tid有该page上的锁
                PageLock pageLock = pageLocks.get(tid);
                if (pageLock.getType() == PageLock.SHARE){
                    // tid 有 page 上的读锁
                    if (requiredType == PageLock.SHARE){
                        //tid 需要获取的是读锁
                        //System.out.println(thread + ": the " + pageId + " have one lock with same txid, transaction" + tid + " require " + lockType + ", accept");
                        return true;
                    }
                    if (requiredType == PageLock.EXCLUSIVE){
                        //tid 需要获取的是写锁
                        if(pageLocks.size() == 1){
                            // 该page上 只有tid的 读锁，则可以将其升级为写锁
                            pageLock.setType(PageLock.EXCLUSIVE);
                            pageLocks.put(tid,pageLock);
                            //System.out.println(thread + ": the " + pageId + " have read lock with same txid, transaction" + tid + " require write lock, accept and upgrade!!!");
                            return true;
                        }
                        if (pageLocks.size() > 1){
                            // 该page 上还有其他事务的锁，则不能升级
                            System.out.println(thread + ": the " + pageId + " have many read locks, transaction" + tid + " require write lock, abort!!!");
                            throw new TransactionAbortedException();
                        }
                    }
                }
                if (pageLock.getType() == PageLock.EXCLUSIVE){
                    // tid 有 page上的写锁
                    //System.out.println(thread + ": the " + pageId + " have write lock with same txid, transaction" + tid + " require " + lockType + ", accept");
                    return true;
                }
            }

            System.out.println("----------------------------------------------------");
            return false;
        }

        /**
         * 检查某一事务在某一page上是否持有锁
         * @param tid 事务id
         * @param pid pageId
         * @return
         */
        public synchronized boolean isholdLock(TransactionId tid, PageId pid){
            ConcurrentHashMap<TransactionId,PageLock> pageLocks;
            pageLocks = lockMap.get(pid);
            if(pageLocks == null){
                return false;
            }
            PageLock pageLock = pageLocks.get(tid);
            if(pageLock == null){
                return false;
            }
            return true;
        }

        /**
         * 释放某一事务在page上的所有锁
         * @param tid 事务id
         * @param pid pageId
         * @return
         */
        public synchronized boolean releaseLock(TransactionId tid, PageId pid){
            if (isholdLock(tid,pid)){
                ConcurrentHashMap<TransactionId,PageLock> pageLocks = lockMap.get(pid);
                pageLocks.remove(tid);
                if (pageLocks.size() == 0){
                    lockMap.remove(pid);
                }
                this.notifyAll();
                return true;
            }
            return false;
        }

        /**
         * 事务完成释放所有锁
         * @param tid 事务id
         */
        public synchronized void completeTransaction(TransactionId tid){
            Set<PageId> pageIds = lockMap.keySet();
            for (PageId pageId : pageIds){
                releaseLock(tid,pageId);
            }
            //System.out.println(Thread.currentThread().getName() + "  transaction" + tid + "   release the lock on the all locks");
        }





    }


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        bufferPool = new HashMap<>();
        head = new LinkedNode();
        tail = new LinkedNode();
        head.next = tail;
        tail.next = head;
        lockManager = new LockManager();
    }

    /**
     * 获得bufferpool的size
     * @return size
     */
    public int getBufferPoolSize() {
        int n=0;
        while(head!=tail) {
            n++;
            head = head.next;
        }
        return n-1;
    }

    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        int lockType;
        if (perm == Permissions.READ_ONLY) {
            lockType = PageLock.SHARE;
        } else {
            lockType = PageLock.EXCLUSIVE;
        }
        long st = System.currentTimeMillis();
        boolean isacquired = false;
        while (!isacquired) {
            try {
                isacquired = lockManager.acquireLock(pid,tid,lockType);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long now = System.currentTimeMillis();
            if (now - st > 500) {
                throw new TransactionAbortedException();
            }
        }
        if(!bufferPool.containsKey(pid)){
            DbFile databaseFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = databaseFile.readPage(pid);
            LinkedNode linkedNode = new LinkedNode(pid, page);

            if(numPages>bufferPool.size()){
                addToHead(linkedNode);
                bufferPool.put(pid,linkedNode);
                return page;
            }else{
                evictPage();
                addToHead(linkedNode);
                bufferPool.put(pid,linkedNode);
                return page;

            }
        }else {
            LinkedNode linkedNode = bufferPool.get(pid);
            moveToHead(linkedNode);
            return linkedNode.page;
        }

    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid,pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pageList = databaseFile.insertTuple(tid, t);
        for(Page page:pageList) {
            PageId id = page.getId();
            page.markDirty(true,tid);
            if(!bufferPool.containsKey(id)) {
                LinkedNode linkedNode = new LinkedNode(id, page);
                int bufferPoolSize = getBufferPoolSize();
                if(getBufferPoolSize() < numPages) {
                    addToHead(linkedNode);
                    bufferPool.put(id,linkedNode);
                } else {
                    evictPage();
                    addToHead(linkedNode);
                    bufferPool.put(id,linkedNode);
                }
            } else {
                LinkedNode linkedNode = bufferPool.get(id);
                addToHead(linkedNode);
                linkedNode.page = page;
                bufferPool.put(id,linkedNode);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile databaseFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pageList = databaseFile.deleteTuple(tid, t);
        for(Page page : pageList) {
            page.markDirty(true,tid);
            LinkedNode linkedNode = bufferPool.get(page.getId());
            if(linkedNode==null) {
                if(getBufferPoolSize()<numPages) {
                    LinkedNode temp = new LinkedNode(page.getId(), page);
                    addToHead(temp);
                    bufferPool.put(page.getId(),temp);
                } else {
                    evictPage();
                    LinkedNode temp = new LinkedNode(page.getId(), page);
                    addToHead(temp);
                    bufferPool.put(page.getId(),temp);
                }
            } else {
                linkedNode.page = page;
                addToHead(linkedNode);
                bufferPool.put(page.getId(),linkedNode);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        bufferPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = bufferPool.get(pid).page;
        TransactionId dirty = page.isDirty();
        if (dirty != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false,null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        for (int i=0; i<numPages; i++){
            LinkedNode tail = removeTail();
            Page evictPage = tail.page;
            if (evictPage.isDirty() != null){
                addToHead(tail);
            }
            else{
                PageId evictPageId = tail.pageId;
                discardPage(evictPageId);
                return;
            }
        }
        throw new DbException("all pages are dirty page ");

    }

}
