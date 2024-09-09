package simpledb.common;

import org.jetbrains.annotations.NotNull;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;
import simpledb.util.Preconditions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * <p>
 * The Catalog keeps track of all available tables in the database and their associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * </p>
 * <p>
 * The content of {@link tableIdToTableMap} and {@link tableNameToFileIdMap} should be consistent.
 * </p>
 *
 * @Threadsafe ToDo: not thread-safe yet
 */
public class Catalog {
    private final Map<Integer, Table> tableIdToTableMap = new HashMap<>();
    private final Map<String, Integer> tableNameToFileIdMap = new HashMap<>();

    /**
     * ToDo: should guarantee a single instance
     */
    Catalog() {
        // some code goes here
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     *
     * @param file      the contents of the table to add;  file.getId() is the identfier of
     *                  this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name      the name of the table -- may be an empty string.  May not be null.  If a name
     *                  conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(@NotNull DbFile file, String name, String pkeyField) {
        Integer existingTableId = tableNameToFileIdMap.remove(name);
        if (existingTableId != null) {
            tableIdToTableMap.remove(existingTableId);
        }
        tableNameToFileIdMap.put(name, file.getId());
        tableIdToTableMap.put(file.getId(), new Table(file, name, pkeyField));
    }

    public void addTable(@NotNull DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     *
     * @param file the contents of the table to add;  file.getId() is the identifier of
     *             this file/tupleDesc param for the calls getTupleDesc and getFile
     */
    public void addTable(@NotNull DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        Integer tableId = tableNameToFileIdMap.get(name);
        Preconditions.checkValueExists(tableId);
        return tableId;
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableId The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableId) throws NoSuchElementException {
        return getTable(tableId).dbFile().getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     *
     * @param tableId The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    @NotNull
    public DbFile getDatabaseFile(int tableId) throws NoSuchElementException {
        return getTable(tableId).dbFile();
    }

    public String getPrimaryKey(int tableId) {
        return getTable(tableId).primaryKeyField();
    }

    @NotNull
    private Table getTable(int tableId) {
        Table table = tableIdToTableMap.get(tableId);
        Preconditions.checkValueExists(table);
        return table;
    }

    public Iterator<Integer> tableIdIterator() {
        return tableIdToTableMap.keySet().iterator();
    }

    public String getTableName(int id) {
        Table table = getTable(id);
        return table.name();
    }

    /**
     * Delete all tables from the catalog
     */
    public void clear() {
        tableIdToTableMap.clear();
        tableNameToFileIdMap.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     *
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = null;
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));

            while ((line = br.readLine()) != null) {
                // assume line is of the format "name (field type, field type, ...)"
                String name = line.substring(0, line.indexOf("(")).trim();
                String fieldLine = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] fields = fieldLine.split(",");
                String[] names = new String[fields.length];
                Type[] types = new Type[fields.length];
                int index = 0;
                String primaryKey = "";
                for (String field : fields) {
                    String[] fieldParams = field.trim().split(" ");
                    names[index] = fieldParams[0].trim();
                    if (fieldParams[1].trim().equalsIgnoreCase("int")) {  // ToDo: add const
                        types[index] = Type.INT_TYPE;
                    } else if (fieldParams[1].trim().equalsIgnoreCase("string")) {
                        types[index] = Type.STRING_TYPE;
                    } else {
                        System.out.println("Unknown type " + fieldParams[1]);   // ToDo: add logger
                        System.exit(0);
                    }
                    if (fieldParams.length == 3) {
                        if (fieldParams[2].trim().equals("pk")) {
                            primaryKey = fieldParams[0].trim();
                        } else {
                            System.out.println("Unknown annotation " + fieldParams[2]);
                            System.exit(0);
                        }
                    }
                    index++;
                }
                TupleDesc t = new TupleDesc(types, names);
                HeapFile heapFile = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(heapFile, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }

    private record Table(@NotNull DbFile dbFile, String name, String primaryKeyField) {
    }
}

