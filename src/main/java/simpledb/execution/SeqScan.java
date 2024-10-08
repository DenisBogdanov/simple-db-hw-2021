package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.Serial;
import java.util.NoSuchElementException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {
    @Serial
    private static final long serialVersionUID = 1L;

    private final TransactionId transactionId;
    private final DbFileIterator dbFileIterator;
    private int tableId;
    private String tableAlias;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableId    The table to scan.
     * @param tableAlias The alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableId, String tableAlias) {
        this.transactionId = tid;
        this.tableId = tableId;
        this.tableAlias = tableAlias;
        this.dbFileIterator = Database.getCatalog().getDatabaseFile(tableId).iterator(transactionId);
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    /**
     * @return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return the alias of the table this operator scans.
     */
    public String getAlias() {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableId = tableid;
        this.tableAlias = tableAlias;
        // ToDo: should the iterator be reset too?
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        dbFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    @Override
    public TupleDesc getTupleDesc() {
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(tableId);
        int n = tupleDesc.numFields();
        Type[] types = new Type[n];
        String[] fields = new String[n];
        for (int i = 0; i < n; i++) {
            types[i] = tupleDesc.getFieldType(i);
            fields[i] = tableAlias + "." + tupleDesc.getFieldName(i);
        }
        return new TupleDesc(types, fields);
    }

    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
        return dbFileIterator.hasNext();
    }

    @Override
    public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
        return dbFileIterator.next();
    }

    @Override
    public void close() {
        dbFileIterator.close();
    }

    @Override
    public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
        dbFileIterator.rewind();
    }
}
