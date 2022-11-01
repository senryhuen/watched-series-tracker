package com.sqlmanager;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * A class for interacting with databases (SQLite).
 * 
 * <p>
 * This class contains methods which format and execute SQL commands.
 * </p>
 * 
 * @author senryhuen
 */
public class SQLManager {

    public final String dbPath;
    private final String DB_URL;
    private Connection conn = null;

    /**
     * Initialises <code>SQLManager</code> instance and formats <code>DB_URL</code>
     * which locates the database.
     * 
     * @param dbPath Path locating database to connect to or create (eg.
     *               "src/main/java/com/example/example.db")
     * @throws SQLException
     */
    public SQLManager(String dbPath) throws SQLException {
        this.dbPath = dbPath;
        this.DB_URL = "jdbc:sqlite:" + this.dbPath;
        connectToDB();
    }

    /**
     * Connects to database specified by <code>DB_URL</code>, creating it if it does
     * not already exist.
     * 
     * @throws SQLException
     */
    public void connectToDB() throws SQLException {
        conn = DriverManager.getConnection(DB_URL);
        enableForeignKey();
    }

    /**
     * Closes connection to database at <code>DB_URL</code>. Basically a destructor.
     * 
     * @throws SQLException
     */
    public void closeConnection() throws SQLException {
        conn.close();
    }

    /**
     * Enable SQLite foreign key support for current connection.
     * 
     * @throws SQLException
     */
    private void enableForeignKey() throws SQLException {
        String sql = "PRAGMA foreign_keys = ON;";
        executeSQLUpdate(sql);
    }

    /**
     * Puts SQL statement as a <code>String</code> into a prepared statement.
     * 
     * @param sql SQL statement to form prepared statement
     * @return <code>PreparedStatement</code> of <code>sql</code> argument
     * @throws SQLException
     */
    private PreparedStatement createPreparedStatement(String sql) throws SQLException {
        return conn.prepareStatement(sql);
    }

    /**
     * Executes an SQL query in the form of a <code>String</code>. Allows other
     * classes to execute SQL queries, without allowing them to execute any updates
     * (changes) to the database.
     * 
     * @param sql SQL query compatible with SQLite3
     * @return Results from <code>sql</code> query
     * @throws SQLException
     */
    public ResultSet getQueryResults(String sql) throws SQLException {
        return createPreparedStatement(sql).executeQuery();
    }

    /**
     * Executes an SQL data manipulation statement in the form of a
     * <code>String</code>.
     * 
     * @param sql SQL data manipulation statement compatible with SQLite3
     * @throws SQLException
     */
    private void executeSQLUpdate(String sql) throws SQLException {
        createPreparedStatement(sql).executeUpdate();
    }

    /**
     * Starts a transaction by disabling autocommit. All statements executed between
     * calling this method and {@link #endTransaction()} will be part of one
     * transaction.
     * 
     * @throws SQLException
     */
    public void startTransaction() throws SQLException {
        conn.setAutoCommit(false);
    }

    /**
     * Ends a transaction by commiting transaction then re-enabling autocommit.
     * 
     * @throws SQLException
     */
    public void endTransaction() throws SQLException {
        conn.commit();
        conn.setAutoCommit(true);
    }

    /**
     * Rollback then cancel transaction by re-enabling autocommit.
     * 
     * @throws SQLException
     */
    public void rollbackTransaction() throws SQLException {
        conn.rollback();
        conn.setAutoCommit(true);
    }

    /**
     * Adds new table to database specified by <code>DB_URL</code>, with just a
     * single column for the primary key. Primary key column constraints are "INT
     * NOT NULL PRIMARY KEY".
     * 
     * <p>
     * Currently does not support composite keys.
     * </p>
     * 
     * @param tableName      Name of the table to be created
     * @param primaryKeyName Name of the primary key column in table to be created -
     *                       can also contain constraints (same format as SQL
     *                       statement) as long as there are no conflicts with "INT
     *                       NOT NULL PRIMARY KEY"
     * @throws SQLException If table with <code>tableName</code> already exists in
     *                      database, or other SQL errors
     */
    public void addBareTableToDB(String tableName, String primaryKeyName) throws SQLException {
        String sql = String.format("CREATE TABLE %s (%s INT NOT NULL PRIMARY KEY)", tableName, primaryKeyName);
        executeSQLUpdate(sql);
    }

    /**
     * Adds new table to database specified by <code>DB_URL</code>, with columns
     * defined by <code>String columns</code>.
     *
     * @param tableName Name of the table to be created
     * @param columns   Comma separated string of column names + contraints. Eg.
     *                  <code>"col1 INT NOT NULL, col2 TEXT"</code>
     * @throws SQLException If table with <code>tableName</code> already exists in
     *                      database, or other SQL errors
     */
    public void addTableToDB(String tableName, String columns) throws SQLException {
        String sql = String.format("CREATE TABLE %s (%s);", tableName, columns);
        executeSQLUpdate(sql);
    }

    /**
     * @return <code>ArrayList</code> of all tables in the database specified by
     *         <code>DB_URL</code>
     * @throws SQLException
     */
    public List<String> getAllTables() throws SQLException {
        DatabaseMetaData DBMetaData = conn.getMetaData();
        ResultSet results = DBMetaData.getTables(null, null, null, null);

        List<String> tableNames = new ArrayList<>();
        while (results.next()) {
            tableNames.add(results.getString("TABLE_NAME"));
        }

        return tableNames;
    }

    /**
     * Checks whether <code>tableName</code> exists in database.
     * 
     * @param tableName Name of table to check for
     * @return <code>true</code> if table exists, otherwise <code>false</code>
     * @throws SQLException
     */
    public boolean checkTableExists(String tableName) throws SQLException {
        List<String> tableNames = getAllTables();
        if (tableNames.contains(tableName)) {
            return true;
        }

        return false;
    }

    /**
     * Renames a table in database.
     * 
     * @param oldTableName Current name of table to rename
     * @param newTablename Name to rename <code>oldTablename</code> to
     * @throws SQLException
     */
    public void renameTable(String oldTableName, String newTablename) throws SQLException {
        String sql = String.format("ALTER TABLE %s RENAME TO %s", oldTableName, newTablename);
        executeSQLUpdate(sql);
    }

    /**
     * Deletes/drops a table from database.
     * 
     * @param tablename Name of table to delete
     * @throws SQLException
     */
    public void removeTable(String tablename) throws SQLException {
        String sql = String.format("DROP TABLE %s", tablename);
        executeSQLUpdate(sql);
    }

    /**
     * Checks whether <code>tableName</code> has a column of
     * <code>columnName</code>.
     * 
     * @param tableName  Name of table to check in
     * @param columnName Name of column to check for
     * @return <code>true</code> if <code>columnName</code> exists in
     *         <code>tableName</code>,
     *         otherwise <code>false</code>
     * @throws SQLException
     */
    public boolean checkTableContainsColumn(String tableName, String columnName) throws SQLException {
        try {
            calcColumnLength(tableName, columnName);
            return true;
        } catch (Exception e) {
            if (e.toString().contains("no such column: ")) {
                return false;
            }
            throw e;
        }
    }

    /**
     * Adds new column of type <code>String</code> (<code>TEXT</code> in SQLite) to
     * a table.
     * 
     * @param tableName  Name of table (in database specified by
     *                   <code>DB_URL</code>) which column is added to
     * @param columnName Name of column to add
     * @throws SQLException If column with <code>columnName</code> already exists in
     *                      table, or other SQL errors
     */
    public void addStringTypeColumnToTable(String tableName, String columnName) throws SQLException {
        String sql = String.format("ALTER TABLE %s ADD %s TEXT", tableName, columnName);
        executeSQLUpdate(sql);
    }

    /**
     * Adds new column of type <code>int</code> to a table.
     * 
     * @param tableName  Name of table (in database specified by
     *                   <code>DB_URL</code>) which column is added to
     * @param columnName Name of column to add
     * @throws SQLException If column with <code>columnName</code> already exists in
     *                      table, or other SQL errors
     */
    public void addIntTypeColumnToTable(String tableName, String columnName) throws SQLException {
        String sql = String.format("ALTER TABLE %s ADD %s INT", tableName, columnName);
        executeSQLUpdate(sql);
    }

    /**
     * Adds new column of type <code>boolean</code> to a table.
     * 
     * <p>
     * SQLite database has no <code>boolean</code> type. It is represented by 0 for
     * <code>false</code>, 1 for <code>true</code>. The new column added will have a
     * default value of 0, and will only accept values of 0 or 1.
     * </p>
     * 
     * @param tableName  Name of table (in database specified by
     *                   <code>DB_URL</code>) which column is added to
     * @param columnName Name of column to add
     * @throws SQLException If column with <code>columnName</code> already exists in
     *                      table, or other SQL errors
     */
    public void addBooleanTypeColumnToTable(String tableName, String columnName) throws SQLException {
        String sql = String.format("ALTER TABLE %s ADD %s INT DEFAULT 0 CHECK(%s==0 OR %s==1)", tableName, columnName,
                columnName, columnName);
        executeSQLUpdate(sql);
    }

    /**
     * Adds new row to a table, with only one attribute being defined. Other
     * attributes can be updated later using the
     * {@link #setValueInCell(String, String, String, String, String)
     * setValueInCell} method.
     * 
     * <p>
     * The column being defined must be the primary key column, unless there is an
     * auto-incremented primary key column as every row requires a primary key.
     * </p>
     * 
     * @param tableName   Name of table (in database specified by
     *                    <code>DB_URL</code>) which row is added to
     * @param columnName  Name of the column which value should be assigned to
     * @param columnValue Value being defined when creating new row
     * @throws SQLException If <code>value</code> is a primary key, and an identical
     *                      key already exists in <code>tableName</code>
     */
    public void addNewRecordToTable(String tableName, String columnName, String value) throws SQLException {
        String sql = String.format("INSERT INTO %s (%s) " + "VALUES (?)", tableName, columnName);

        try (PreparedStatement ps = createPreparedStatement(sql)) {
            ps.setString(1, value);
            ps.executeUpdate();
        }
    }

    /**
     * Updates value in an existing cell identified by its row and column in
     * <code>tableName</code>.
     * 
     * @param tableName       Name of table (in database specified by
     *                        <code>DB_URL</code>) which contains the cell being
     *                        updated
     * @param primaryKeyValue Value of the primary key attribute of row which
     *                        contains the cell being updated
     * @param columnName      Name of column that is being updated
     * @param value           The value to update the cell to
     * @throws SQLException
     */
    public void setValueInCell(String tableName, String primaryKeyValue, String columnName, String value)
            throws SQLException {
        String primaryKeyName = getPrimaryKeyColumnName(tableName);
        String sql = String.format("UPDATE %s SET %s = ? WHERE %s = ?", tableName, columnName, primaryKeyName);

        try (PreparedStatement ps = createPreparedStatement(sql)) {
            ps.setString(1, value);
            ps.setString(2, primaryKeyValue);
            ps.executeUpdate();
        }
    }

    /**
     * Indentical to
     * {@link #setValueInCell(String, String, String, String, String)}, but
     * <code>primaryKeyValue</code> is an integer rather than string.
     * 
     * @see #setValueInCell(String, String, String, String, String)
     */
    public void setValueInCell(String tableName, int primaryKeyValue, String columnName, String value)
            throws SQLException {
        String primaryKeyName = getPrimaryKeyColumnName(tableName);
        String sql = String.format("UPDATE %s SET %s = ? WHERE %s = ?", tableName, columnName, primaryKeyName);

        try (PreparedStatement ps = createPreparedStatement(sql)) {
            ps.setString(1, value);
            ps.setInt(2, primaryKeyValue);
            ps.executeUpdate();
        }
    }

    /**
     * Gets value in a cell as a <code>String</code>.
     * 
     * @param tableName       Name of table which contains the desired cell
     * @param primaryKeyValue Value of the primary key attribute which identifies
     *                        the row that contains the desired cell
     * @param columnName      Name of the desired column
     * @return Value at cell identified by <code>primaryKeyValue</code> and
     *         <code>columnName</code> in <code>tableName</code>
     * @throws SQLException
     */
    public String getValueInCell(String tableName, int primaryKeyValue, String columnName) throws SQLException {
        String primaryKeyName = getPrimaryKeyColumnName(tableName);
        String sql = String.format("SELECT %s FROM %s WHERE %s = ?", columnName, tableName, primaryKeyName);

        try (PreparedStatement ps = createPreparedStatement(sql)) {
            ps.setInt(1, primaryKeyValue);
            return ps.executeQuery().getString(1);
        }
    }

    /**
     * Indentical to {@link #getValueInCell(String, int, String)}, but
     * <code>primaryKeyValue</code> is a string rather than an integer.
     */
    public String getValueInCell(String tableName, String primaryKeyValue, String columnName) throws SQLException {
        String primaryKeyName = getPrimaryKeyColumnName(tableName);
        String sql = String.format("SELECT %s FROM %s WHERE %s = ?", columnName, tableName, primaryKeyName);

        try (PreparedStatement ps = createPreparedStatement(sql)) {
            ps.setString(1, primaryKeyValue);
            return ps.executeQuery().getString(1);
        }
    }

    /**
     * Removes/Drops/Deletes a row from a table.
     * 
     * @param tableName       Table to remove row from
     * @param primaryKeyValue Value of the primary key attribute of row to be
     *                        removed
     * @throws SQLException
     */
    public void removeFromTable(String tableName, int primaryKeyValue) throws SQLException {
        String primaryKeyName = getPrimaryKeyColumnName(tableName);
        String sql = String.format("DELETE FROM %s WHERE %s = ?", tableName, primaryKeyName);

        try (PreparedStatement ps = createPreparedStatement(sql)) {
            ps.setInt(1, primaryKeyValue);
            ps.executeUpdate();
        }
    }

    /**
     * Indentical to
     * {@link #removeFromTable(String, int)}, but
     * <code>primaryKeyValue</code> is a String rather than an integer.
     * 
     * @see #removeFromTable(String, int)
     */
    public void removeFromTable(String tableName, String primaryKeyValue) throws SQLException {
        String primaryKeyName = getPrimaryKeyColumnName(tableName);
        String sql = String.format("DELETE FROM %s WHERE %s = ?", tableName, primaryKeyName);

        try (PreparedStatement ps = createPreparedStatement(sql)) {
            ps.setString(1, primaryKeyValue);
            ps.executeUpdate();
        }
    }

    /**
     * Get name of primary key column for a table.
     * 
     * <p>
     * Intended only for single-column primary keys, will not work with composite
     * keys.
     * </p>
     * 
     * @param tableName Name of table of interest
     * @return Name of column that is the primary key column for
     *         <code>tableName</code>
     * @throws SQLException
     */
    public String getPrimaryKeyColumnName(String tableName) throws SQLException {
        String sql = String.format("PRAGMA table_info(%s)", tableName);

        try (PreparedStatement ps = createPreparedStatement(sql)) {
            ResultSet result = ps.executeQuery();
            while (result.next()) {
                if (result.getInt("pk") == 1) {
                    return result.getString("name");
                }
            }

            // table does not exist (or no primary key column found)
            throw new SQLException(String.format("table '%s' not found", tableName));
        }
    }

    /**
     * Gets a list of all primary keys in a table.
     * 
     * <p>
     * Primary keys must be of data type <code>int</code> or <code>String</code>.
     * </p>
     * 
     * @param tableName Name of table to get list of primary keys of
     * @return <code>ArrayList</code> of all primary keys in <code>tableName</code>
     *         as <code>String</code>s
     * @throws SQLException
     */
    public List<String> getAllPrimaryKeys(String tableName) throws SQLException {
        String primaryKeyName = getPrimaryKeyColumnName(tableName);
        String sql = String.format("SELECT %s FROM %s;", primaryKeyName, tableName);

        try (ResultSet result = getQueryResults(sql)) {
            List<String> allPrimaryKeys = new ArrayList<>();

            while (result.next()) {
                allPrimaryKeys.add(result.getString(primaryKeyName));
            }

            return allPrimaryKeys;
        }
    }

    /**
     * @param tableName       Name of table to search for
     *                        <code>primaryKeyValue</code>
     * @param primaryKeyValue Value of primary key to search for in
     *                        <code>tableName</code>
     * @return <code>true</code> if <code>primaryKeyValue</code> exists in
     *         <code>tableName</code>, <code>false</code> otherwise
     * @throws SQLException If <code>tableName</code> does not exist, or other
     *                      SQLExceptions
     */
    public boolean checkTableContainsPrimaryKey(String tableName, String primaryKeyValue) throws SQLException {
        List<String> allPrimaryKeys = getAllPrimaryKeys(tableName);
        return allPrimaryKeys.contains(primaryKeyValue);
    }

    /**
     * Count number of non-null rows in a column in a table.
     * 
     * @param tableName  Name of table (in database specified by
     *                   <code>DB_URL</code>) which contains the column being
     *                   counted
     * @param columnName Name of column being counted
     * @return Number of non-null rows in <code>columnName</code> (or null if column
     *         does not exist)
     * @throws SQLException
     */
    public int calcColumnLength(String tableName, String columnName) throws SQLException {
        String sql = String.format("SELECT COUNT(ALL %s) AS count FROM %s;", columnName, tableName);
        return getQueryResults(sql).getInt(1);
    }

    /**
     * Indentical to {@link #calcColumnLength(String, String)}, but takes an
     * additional parameter for filtering column with a condition.
     * 
     * @param condition Condition which would go after the <code>WHERE</code> clause
     *                  in an SQL statement (compatible with SQLite3). eg.
     *                  "primaryKeyColumn=100 AND column=101"
     * @see #calcColumnLength(String, String)
     */
    public int calcColumnLength(String tableName, String columnName, String condition) throws SQLException {
        String sql = String.format("SELECT COUNT(ALL %s) AS count FROM %s WHERE %s;", columnName, tableName, condition);
        return getQueryResults(sql).getInt(1);
    }

    /**
     * Get highest value in a column in a table. Highest value for
     * <code>String</code>s is the last value when sorted alphabetically.
     * 
     * @param tableName  Name of table (in database specified by
     *                   <code>DB_URL</code>) which contains <code>columnName</code>
     * @param columnName Name of column to get <code>MAX</code> of
     * @return Maximum value in <code>columnName</code>, <code>null</code> if
     *         <code>tableName</code> has no rows
     * @throws SQLException
     */
    public String getMaxValueInColumn(String tableName, String columnName) throws SQLException {
        String sql = String.format("SELECT MAX(%s) AS max_val FROM %s;", columnName, tableName);
        return getQueryResults(sql).getString(1);
    }

    /**
     * @param date <code>String</code> to be checked if it is a date in "YYYY-MM-DD"
     *             (ISO_LOCAL_DATE) format
     * @return <code>true</code> if <code>date</code> is formatted in ISO_LOCAL_DATE
     *         correctly, <code>false</code> if otherwise
     */
    public static boolean isISOLocalDateFormat(String date) {
        try {
            LocalDate.parse(date);
        } catch (DateTimeParseException e) {
            return false;
        }

        return true;
    }

    /**
     * Checks that <code>firstDate</code> and <code>secondDate</code> are in
     * chronological order.
     * 
     * @param firstDate  Date as a <code>String</code> in "YYYY-MM-DD"
     *                   (ISO_LOCAL_DATE) format
     * @param secondDate Date as a <code>String</code> in "YYYY-MM-DD"
     *                   (ISO_LOCAL_DATE) format
     * @return <code>false</code> if <code>secondDate</code> comes before
     *         <code>firstDate</code>, <code>true</code> otherwise
     */
    public static boolean isChronologicalOrder(String firstDate, String secondDate) {
        LocalDate firstdate = LocalDate.parse(firstDate);
        LocalDate seconddate = LocalDate.parse(secondDate);

        if (seconddate.compareTo(firstdate) < 0) {
            return false;
        }

        return true;
    }

    /**
     * @return Current date in "YYYY-MM-DD" (ISO_LOCAL_DATE) format
     */
    public static String getCurrentDate() {
        return LocalDate.now().toString();
    }

}
