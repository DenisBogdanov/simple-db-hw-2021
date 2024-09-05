package simpledb.storage;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import simpledb.common.Type;
import simpledb.common.annotations.Immutable;
import simpledb.util.Preconditions;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * <strong>Tuple descriptor</strong> defines the schema of a tuple.
 */
@Immutable
public class TupleDesc implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final List<TDItem> items;
    private final int totalSize;

    /**
     * Create a new {@code TupleDesc} with {@code types.length} fields with specified types and associated field names.
     *
     * @param types  array specifying the number of and types of fields in this {@code TupleDesc}.
     *               It must contain at least one entry.
     * @param fields array specifying the names of the fields. Note that names may be {@code null}.
     */
    public TupleDesc(Type[] types, String[] fields) {
        items = IntStream.range(0, types.length)
                .mapToObj(i -> new TDItem(types[i], fields[i]))
                .toList();

        totalSize = calcSize(types);
    }

    /**
     * Create a new {@code TupleDesc} with {@code types.length} fields with fields of the specified types,
     * with anonymous (unnamed) fields.
     *
     * @param types array specifying the number of and types of fields in this {@code TupleDesc}.
     *              It must contain at least one entry.
     */
    public TupleDesc(Type[] types) {
        items = Arrays.stream(types).map(type -> new TDItem(type, null)).toList();
        totalSize = calcSize(types);
    }

    private TupleDesc(List<TDItem> items, int totalSize) {
        this.items = items;
        this.totalSize = totalSize;
    }

    private static int calcSize(Type[] types) {
        int size = 0;
        for (Type type : types) {
            size += type.getLen();
        }
        return size;
    }

    /**
     * Merge two {@code TupleDesc}s into one, with {@code td1.numFields + td2.numFields} fields,
     * with the first {@code td1.numFields} coming from {@code td1} and the remaining from {@code td2}.
     *
     * @param td1 The {@code TupleDesc} with the first fields of the new {@code TupleDesc}
     * @param td2 The {@code TupleDesc} with the last fields of the {@code TupleDesc}
     * @return the new {@code TupleDesc}
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        List<TDItem> items = Stream.of(td1.items, td2.items)
                .flatMap(List::stream)
                .toList();
        return new TupleDesc(items, td1.getSize() + td2.getSize());
    }

    /**
     * @return An iterator which iterates over all the field TDItems that are included in this {@code TupleDesc}.
     */
    public Iterator<TDItem> iterator() {
        return items.iterator();
    }

    /**
     * @return the number of fields in this {@code TupleDesc}.
     */
    public int numFields() {
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        Preconditions.checkIndex(i, items.size());
        return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        Preconditions.checkIndex(i, items.size());
        return items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < items.size(); i++) {
            if (Objects.equals(items.get(i).fieldName, name)) return i;
        }

        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return totalSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TupleDesc tupleDesc = (TupleDesc) o;
        return items.equals(tupleDesc.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(items);
    }

    @Override
    public String toString() {
        return items.stream().map(String::valueOf).collect(Collectors.joining(","));
    }

    /**
     * Organizes the information of each field.
     */
    @Immutable
    public static class TDItem implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        private final Type fieldType;
        private final String fieldName;

        private TDItem(@NotNull Type fieldType, @Nullable String fieldName) {
            this.fieldType = fieldType;
            this.fieldName = fieldName;
        }

        public String getFieldName() {
            return fieldName;
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TDItem tdItem = (TDItem) o;
            return fieldType == tdItem.fieldType && Objects.equals(fieldName, tdItem.fieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldType, fieldName);
        }
    }
}
