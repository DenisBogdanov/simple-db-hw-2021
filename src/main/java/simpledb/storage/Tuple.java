package simpledb.storage;

import org.jetbrains.annotations.Nullable;
import simpledb.util.Preconditions;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * schema specified by a {@link TupleDesc} object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final Field[] fields;
    private TupleDesc tupleDesc;
    private RecordId recordId;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param tupleDesc the schema of this tuple. It must be a valid TupleDesc
     *                  instance with at least one field.
     */
    public Tuple(TupleDesc tupleDesc) {
        this.tupleDesc = tupleDesc;
        this.fields = new Field[tupleDesc.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk.
     */
    public @Nullable RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param recordId the new RecordId for this tuple.
     */
    public void setRecordId(@Nullable RecordId recordId) {
        this.recordId = recordId;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i     index of the field to change. It must be a valid index.
     * @param field new value for the field.
     */
    public void setField(int i, Field field) {
        Preconditions.checkIndex(i, fields.length);
        fields[i] = field;
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        Preconditions.checkIndex(i, fields.length);
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    @Override
    public String toString() {
        return Arrays.stream(fields).map(String::valueOf).collect(Collectors.joining("\t"));
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        return Arrays.stream(fields).iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        this.tupleDesc = td;
    }
}
