package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    private final File file;
    private final TupleDesc tupleDescription;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param file the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File file, TupleDesc tupleDescription) {
        this.file = file;
        this.tupleDescription = tupleDescription;
    }

    /**
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableId somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapFile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return tupleDescription;
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) {
        int pageSize = BufferPool.getPageSize();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");) {
            randomAccessFile.seek((long) pid.getPageNumber() * pageSize);
            byte[] data = new byte[pageSize];
            randomAccessFile.read(data);
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), data);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    // see DbFile.java for javadocs
    @Override
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.getPageSize());
    }

    @Override
    public List<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    @Override
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    @Override
    public DbFileIterator iterator(TransactionId tid) {
        int heapFileId = getId();
        int numPages = this.numPages();

        return new AbstractDbFileIterator() {
            private int pageIndex;
            private Iterator<Tuple> iterator;
            private boolean isOpened = false;

            @Override
            protected Tuple readNext() throws DbException, TransactionAbortedException {
                if (!isOpened) return null;
                if (iterator != null && iterator.hasNext()) return iterator.next();
                if (pageIndex >= numPages) return null;
                HeapPageId pageId = new HeapPageId(heapFileId, pageIndex);
                pageIndex++;
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                iterator = page.iterator();
                return iterator.next();
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pageIndex = 0;
                isOpened = true;
            }

            @Override
            public void close() {
                super.close();
                iterator = null;
                isOpened = false;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }
        };
    }
}
