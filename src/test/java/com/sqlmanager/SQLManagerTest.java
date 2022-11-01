package com.sqlmanager;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.format.DateTimeParseException;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SQLManagerTest {

    static SQLManager sqlReadMan, sqlWriteMan;

    static String readDBFilePath = "src/test/java/com/sqlmanager/testRead.db";
    static String writeDBFilePath = "src/test/java/com/sqlmanager/testWrite.db";

    @BeforeAll
    static void setUp() throws SQLException, IOException {
        // delete test databases if they already exist, start fresh
        File readDBFile = new File(readDBFilePath);
        Files.deleteIfExists(readDBFile.toPath());
        File writeDBFile = new File(writeDBFilePath);
        Files.deleteIfExists(writeDBFile.toPath());

        sqlReadMan = new SQLManager("src/test/java/com/sqlmanager/testRead.db");

        sqlReadMan.addTableToDB("int_pk_table", "int_pk INT PRIMARY KEY, int_col INT, str_col TEXT, all_null_col INT");
        sqlReadMan.addNewRecordToTable("int_pk_table", "int_pk", "1");
        sqlReadMan.addNewRecordToTable("int_pk_table", "int_pk", "2");
        sqlReadMan.addNewRecordToTable("int_pk_table", "int_pk", "3");
        sqlReadMan.addNewRecordToTable("int_pk_table", "int_pk", "4");
        sqlReadMan.addNewRecordToTable("int_pk_table", "int_pk", "5");
        sqlReadMan.setValueInCell("int_pk_table", "1", "int_col", "101");
        sqlReadMan.setValueInCell("int_pk_table", "2", "int_col", "2");
        sqlReadMan.setValueInCell("int_pk_table", "4", "int_col", "101");
        sqlReadMan.setValueInCell("int_pk_table", "5", "int_col", "19");
        sqlReadMan.setValueInCell("int_pk_table", "1", "str_col", "ta");
        sqlReadMan.setValueInCell("int_pk_table", "3", "str_col", "df");
        sqlReadMan.setValueInCell("int_pk_table", "4", "str_col", "ta");
        sqlReadMan.setValueInCell("int_pk_table", "5", "str_col", "ac");

        sqlReadMan.addTableToDB("str_pk_table", "str_pk TEXT PRIMARY KEY, int_col INT, str_col TEXT, all_null_col INT");
        sqlReadMan.addNewRecordToTable("str_pk_table", "str_pk", "pk1");
        sqlReadMan.addNewRecordToTable("str_pk_table", "str_pk", "pk2");
        sqlReadMan.addNewRecordToTable("str_pk_table", "str_pk", "pk3");
        sqlReadMan.addNewRecordToTable("str_pk_table", "str_pk", "pk4");
        sqlReadMan.addNewRecordToTable("str_pk_table", "str_pk", "pk5");
        sqlReadMan.setValueInCell("str_pk_table", "pk1", "int_col", "101");
        sqlReadMan.setValueInCell("str_pk_table", "pk2", "int_col", "2");
        sqlReadMan.setValueInCell("str_pk_table", "pk4", "int_col", "101");
        sqlReadMan.setValueInCell("str_pk_table", "pk5", "int_col", "19");
        sqlReadMan.setValueInCell("str_pk_table", "pk1", "str_col", "ta");
        sqlReadMan.setValueInCell("str_pk_table", "pk3", "str_col", "df");
        sqlReadMan.setValueInCell("str_pk_table", "pk4", "str_col", "ta");
        sqlReadMan.setValueInCell("str_pk_table", "pk5", "str_col", "ac");

        sqlReadMan.addTableToDB("no_rows_table", "int_pk INT PRIMARY KEY, int_col INT, str_col TEXT, all_null_col INT");

        sqlWriteMan = new SQLManager("src/test/java/com/sqlmanager/testWrite.db");
        sqlWriteMan.addTableToDB("int_pk_table", "int_pk INT PRIMARY KEY, int_col INT, str_col TEXT");
        sqlWriteMan.addTableToDB("str_pk_table", "str_pk TEXT PRIMARY KEY, int_col INT, str_col TEXT");
        sqlWriteMan.addTableToDB("autoinc_table",
                "autoinc_pk INTEGER PRIMARY KEY AUTOINCREMENT, int_col INT, str_col TEXT");
    }

    @AfterAll
    static void tearDown() throws SQLException {
        File readDBFile = new File(readDBFilePath);
        if (!readDBFile.delete()) {
            throw new RuntimeException(String.format("Failed to delete '%s' and '%s'. Please manually delete.",
                    readDBFilePath, writeDBFilePath));
        }

        File writeDBFile = new File(writeDBFilePath);
        if (!writeDBFile.delete()) {
            throw new RuntimeException(
                    String.format("Failed to delete '%s'. Please manually delete.", writeDBFilePath));
        }
    }

    // whenGettingQueryResults

    @Test
    void givenValidSQLQueryString_whenGettingQueryResult_thenCorrectResult() throws SQLException {
        ResultSet result = sqlReadMan.getQueryResults("SELECT int_col FROM int_pk_table;");
        assertEquals(101, result.getInt(1));
    }

    @Test
    void givenValidSQLUpdateString_whenGettingQueryResult_thenThrowsSQLException() {
        // query should not allow any modifications to the database
        assertThrows(SQLException.class, () -> {
            sqlReadMan.getQueryResults("UPDATE int_pk_table SET int_col = 1 WHERE int_pk = 2;");
        });
    }

    @Test
    void givenInvalidSQLQueryString_whenGettingQueryResult_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.getQueryResults("not an sql statement;");
        });
    }

    @Test
    void givenEmptySQLQueryString_whenGettingQueryResult_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.getQueryResults("");
        });
    }

    // whenAddingBareTable

    @Test
    void givenValidTablenameAndPrimaryKeyName_whenAddingBareTable_thenSuccess() throws SQLException {
        assertFalse(sqlWriteMan.checkTableExists("bare_table"));
        sqlWriteMan.addBareTableToDB("bare_table", "valid_pk");
        assertTrue(sqlWriteMan.checkTableExists("bare_table"));
    }

    @Test
    void givenClashingTablename_whenAddingBareTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addBareTableToDB("existing_bare_table", "valid_pk");
            sqlWriteMan.addBareTableToDB("existing_bare_table", "also_valid_pk");
        });
    }

    @Test
    void givenEmptyTablenameString_whenAddingBareTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addBareTableToDB("", "valid_pk");
        });
    }

    // whenAddingTableWithColumns

    @Test
    void givenValidTablenameAndColumns_whenAddingTableWithColumns_thenSuccess() throws SQLException {
        assertFalse(sqlWriteMan.checkTableExists("table_with_columns"));
        sqlWriteMan.addTableToDB("table_with_columns", "first_col INT, second_col TEXT");
        assertTrue(sqlWriteMan.checkTableExists("table_with_columns"));
    }

    @Test
    void givenValidTablenameAndInvalidColumns_whenAddingTableWithColumns_thenThrowsSQLException() throws SQLException {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addTableToDB("invalid_table_with_columns", "not valid columns");
        });

        assertFalse(sqlWriteMan.checkTableExists("invalid_table_with_columns"));
    }

    @Test
    void givenValidTablenameAndEmptyColumnsString_whenAddingTableWithColumns_thenThrowsSQLException()
            throws SQLException {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addTableToDB("also_invalid_table_with_columns", "");
        });

        assertFalse(sqlWriteMan.checkTableExists("also_invalid_table_with_columns"));
    }

    @Test
    void givenClashingTablename_whenAddingTableWithColumns_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addTableToDB("existing_table_with_columns", "first_col INT, second_col TEXT");
            sqlWriteMan.addTableToDB("existing_table_with_columns", "first_col INT, second_col TEXT");
        });
    }

    @Test
    void givenEmptyTablenameString_whenAddingTableWithColumns_thenSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addTableToDB("", "first_col INT, second_col TEXT");
        });
    }

    // whenGettingAllTablenames

    @Test
    void givenTablesExist_whenGettingAllTablenames_thenCorrectResult() throws SQLException {
        List<String> tableNames = sqlReadMan.getAllTables();
        assertEquals(3, tableNames.size());
        assertTrue(tableNames.contains("int_pk_table"));
        assertTrue(tableNames.contains("str_pk_table"));
        assertTrue(tableNames.contains("no_rows_table"));
    }

    // whenCheckingTableExists

    @Test
    void givenTableExists_whenCheckingTableExists_thenReturnsTrue() throws SQLException {
        assertTrue(sqlReadMan.checkTableExists("int_pk_table"));
    }

    @Test
    void givenTableNotExists_whenCheckingTableExists_thenReturnsFalse() throws SQLException {
        assertFalse(sqlReadMan.checkTableExists("non_existant_table"));
    }

    // whenCheckingColumnExists

    @Test
    void givenTableNotExists_whenCheckingColumnExists_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.checkTableContainsColumn("non_existant_table", "int_col");
        });
    }

    @Test
    void givenEmptyTablenameString_whenCheckingColumnExists_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.checkTableContainsColumn("", "int_col");
        });
    }

    @Test
    void givenTableAndColumnExists_whenCheckingColumnExists_thenReturnsTrue() throws SQLException {
        assertTrue(sqlReadMan.checkTableContainsColumn("int_pk_table", "int_col"));
    }

    @Test
    void givenTableAndColumnExistsAndTableHasNoRows_whenCheckingColumnExists_thenReturnsTrue() throws SQLException {
        assertTrue(sqlReadMan.checkTableContainsColumn("no_rows_table", "int_col"));
    }

    @Test
    void givenTableExistsAndColumnNotExist_whenCheckingColumnExists_thenReturnsFalse() throws SQLException {
        assertFalse(sqlReadMan.checkTableContainsColumn("int_pk_table", "non_existant_col"));
    }

    @Test
    void givenTableExistsAndEmptyColumnnameString_whenCheckingColumnExists_thenReturnsFalse() throws SQLException {
        sqlReadMan.checkTableContainsColumn("int_pk_table", "");
    }

    // whenAddingStringColumnToTable

    @Test
    void givenTableNotExists_whenAddingStringColumnToTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addStringTypeColumnToTable("non_existant_table", "new_col");
        });
    }

    @Test
    void givenEmptyTablenameString_whenAddingStringColumnToTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addStringTypeColumnToTable("", "new_col");
        });
    }

    @Test
    void givenClashingColumnname_whenAddingStringColumnToTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addStringTypeColumnToTable("int_pk_table", "clashing_string_col");
            sqlWriteMan.addStringTypeColumnToTable("int_pk_table", "clashing_string_col");
        });
    }

    @Test
    void givenTableExistsAndColumnNotExists_whenAddingStringColumnToTable_thenSuccess() throws SQLException {
        assertFalse(sqlWriteMan.checkTableContainsColumn("int_pk_table", "new_string_col"));
        sqlWriteMan.addStringTypeColumnToTable("int_pk_table", "new_string_col");
        assertTrue(sqlWriteMan.checkTableContainsColumn("int_pk_table", "new_string_col"));
    }

    // whenAddingIntColumnToTable

    @Test
    void givenTableNotExists_whenAddingIntColumnToTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addIntTypeColumnToTable("non_existant_table", "new_col");
        });
    }

    @Test
    void givenEmptyTablenameString_whenAddingIntColumnToTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addIntTypeColumnToTable("", "new_col");
        });
    }

    @Test
    void givenClashingColumnname_whenAddingIntColumnToTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addStringTypeColumnToTable("int_pk_table", "clashing_int_col");
            sqlWriteMan.addIntTypeColumnToTable("int_pk_table", "clashing_int_col");
        });
    }

    @Test
    void givenTableExistsAndColumnNotExists_whenAddingIntColumnToTable_thenSuccess() throws SQLException {
        assertFalse(sqlWriteMan.checkTableContainsColumn("int_pk_table", "new_int_col"));
        sqlWriteMan.addIntTypeColumnToTable("int_pk_table", "new_int_col");
        assertTrue(sqlWriteMan.checkTableContainsColumn("int_pk_table", "new_int_col"));
    }

    // whenAddingBooleanColumnToTable

    @Test
    void givenTableNotExists_whenAddingBooleanColumnToTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addBooleanTypeColumnToTable("non_existant_table", "new_col");
        });
    }

    @Test
    void givenEmptyTablenameString_whenAddingBooleanColumnToTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addBooleanTypeColumnToTable("", "new_col");
        });
    }

    @Test
    void givenClashingColumnname_whenAddingBooleanColumnToTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addBooleanTypeColumnToTable("int_pk_table", "clashing_boolean_col");
            sqlWriteMan.addBooleanTypeColumnToTable("int_pk_table", "clashing_boolean_col");
        });
    }

    @Test
    void givenTableExistsAndColumnNotExists_whenAddingBooleanColumnToTable_thenSuccess() throws SQLException {
        assertFalse(sqlWriteMan.checkTableContainsColumn("int_pk_table", "new_boolean_col"));
        sqlWriteMan.addBooleanTypeColumnToTable("int_pk_table", "new_boolean_col");
        assertTrue(sqlWriteMan.checkTableContainsColumn("int_pk_table", "new_boolean_col"));
    }

    // whenAddingNewRecordToTable

    @Test
    void givenEmptyTablenameString_whenAddingNewRecordToTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addNewRecordToTable("", "int_col", "1");
        });
    }

    @Test
    void givenTableNotExists_whenAddingNewRecordToTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addNewRecordToTable("non_existant_table", "int_col", "1");
        });
    }

    @Test
    void givenTableExistsAndEmptyColumnnameString_whenAddingNewRecordToTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addNewRecordToTable("int_pk_table", "", "");
        });
    }

    @Test
    void givenTableExistsAndColumnNotExists_whenAddingNewRecordToTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.addNewRecordToTable("int_pk_table", "non_existant_col", "1");
        });
    }

    @Test
    void givenTableNotAutoIncrementsAndColumnIsPrimaryKeyAndValidValue_whenAddingNewRecordToTable_thenSuccess()
            throws SQLException {
        sqlWriteMan.addNewRecordToTable("str_pk_table", "str_pk", "added_row_with_pk");
        assertTrue(sqlWriteMan.checkTableContainsPrimaryKey("str_pk_table", "added_row_with_pk"));
    }

    @Test
    void givenTableNotAutoIncrementsAndColumnNotPrimaryKey_whenAddingNewRecordToTable_thenDoesNothing()
            throws SQLException {
        sqlWriteMan.addNewRecordToTable("str_pk_table", "str_col", "not a primary key");

        // assert record was not added to table
        assertEquals(0, sqlWriteMan.calcColumnLength("str_pk_table", "str_pk", "str_col='not a primary key'"));
    }

    @Test
    void givenTableAutoIncrementsAndColumnNotPrimaryKey_whenAddingNewRecordToTable_thenSuccess() throws SQLException {
        sqlWriteMan.addNewRecordToTable("autoinc_table", "str_col", "added_row_without_pk");

        // assert record was added to table
        assertEquals(1, sqlWriteMan.calcColumnLength("autoinc_table", "autoinc_pk", "str_col='added_row_without_pk'"));
    }

    // whenSettingValueInCell

    @Test
    void givenTableAndPrimaryKeyAndColumnExistsAndValueValid_whenSettingValueInCell_thenSuccess() throws SQLException {
        sqlWriteMan.addNewRecordToTable("str_pk_table", "str_pk", "set_value_in_this_row");
        sqlWriteMan.addNewRecordToTable("int_pk_table", "int_pk", "20");

        sqlWriteMan.setValueInCell("str_pk_table", "set_value_in_this_row", "str_col", "setting_value_in_cell_str_pk");
        sqlWriteMan.setValueInCell("int_pk_table", 20, "str_col", "setting_value_in_cell_int_pk");

        assertEquals("setting_value_in_cell_str_pk",
                sqlWriteMan.getValueInCell("str_pk_table", "set_value_in_this_row", "str_col"));
        assertEquals("setting_value_in_cell_int_pk", sqlWriteMan.getValueInCell("int_pk_table", 20, "str_col"));
    }

    @Test
    void givenEmptyTablenameString_whenSettingValueInCell_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.setValueInCell("", "1", "int_col", "1");
        });

        assertThrows(SQLException.class, () -> {
            sqlWriteMan.setValueInCell("", 1, "int_col", "1");
        });
    }

    @Test
    void givenTableNotExists_whenSettingValueInCell_thenThrowsSQLException() throws SQLException {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.setValueInCell("non_existant_table", "1", "int_col", "1");
        });

        assertThrows(SQLException.class, () -> {
            sqlWriteMan.setValueInCell("non_existant_table", 1, "int_col", "1");
        });
    }

    @Test
    void givenEmptyPrimaryKeyString_whenSettingValueInCell_thenDoesNothing() throws SQLException {
        sqlWriteMan.setValueInCell("str_pk_table", "", "int_col", "1234");
        assertEquals(0, sqlReadMan.calcColumnLength("str_pk_table", "int_col", "int_col==1234"));
    }

    @Test
    void givenPrimaryKeyNotExists_whenSettingValueInCell_thenDoesNothing() throws SQLException {
        sqlWriteMan.setValueInCell("int_pk_table", "3000", "str_col", "non_existant_str_pk");
        assertEquals(0, sqlReadMan.calcColumnLength("int_pk_table", "str_col", "str_col=='non_existant_str_pk'"));

        sqlWriteMan.setValueInCell("int_pk_table", 3000, "str_col", "non_existant_int_pk");
        assertEquals(0, sqlReadMan.calcColumnLength("int_pk_table", "str_col", "str_col=='non_existant_int_pk'"));
    }

    @Test
    void givenEmptyColumnnameString_whenSettingValueInCell_thenThrowsSQLException() throws SQLException {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.setValueInCell("int_pk_table", "1", "", "1");
        });

        assertThrows(SQLException.class, () -> {
            sqlWriteMan.setValueInCell("int_pk_table", 1, "", "1");
        });
    }

    @Test
    void givenColumnNotExists_whenSettingValueInCell_thenThrowsSQLException() throws SQLException {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.setValueInCell("int_pk_table", "1", "non_existant_col", "setting_value_non_existant_col_1");
        });

        assertThrows(SQLException.class, () -> {
            sqlWriteMan.setValueInCell("int_pk_table", 1, "non_existant_col", "setting_value_non_existant_col_2");
        });

        assertFalse(sqlWriteMan.checkTableContainsColumn("int_pk_table", "non_existant_col"));
    }

    @Test
    void givenEmptyValueStringAndStringTypeColumn_whenSettingValueInCell_thenSuccess() throws SQLException {
        sqlWriteMan.addNewRecordToTable("int_pk_table", "int_pk", "2");
        sqlWriteMan.addNewRecordToTable("int_pk_table", "int_pk", "3");

        sqlWriteMan.setValueInCell("int_pk_table", "2", "str_col", "");
        assertEquals("", sqlWriteMan.getValueInCell("int_pk_table", "2", "str_col"));

        sqlWriteMan.setValueInCell("int_pk_table", 3, "str_col", "");
        assertEquals("", sqlWriteMan.getValueInCell("int_pk_table", "3", "str_col"));
    }

    @Test
    void givenEmptyValueStringAndIntTypeColumn_whenSettingValueInCell_thenSuccess() throws SQLException {
        sqlWriteMan.addNewRecordToTable("int_pk_table", "int_pk", "4");

        sqlWriteMan.setValueInCell("int_pk_table", "4", "int_col", "");
        assertEquals("", sqlWriteMan.getValueInCell("int_pk_table", 4, "int_col"));
    }

    @Test
    void givenInvalidValueForBooleanColumn_whenSettingValueInCell_thenThrowsSQLException() throws SQLException {
        sqlWriteMan.addNewRecordToTable("int_pk_table", "int_pk", "5");
        sqlWriteMan.addBooleanTypeColumnToTable("int_pk_table", "bool_col");

        assertThrows(SQLException.class, () -> {
            sqlWriteMan.setValueInCell("int_pk_table", 5, "bool_col", "6");
        });
    }

    // whenGettingValueInCell

    @Test
    void givenTableAndPrimaryKeyAndColumnExists_whenGettingValueInCell_thenSuccess() throws SQLException {
        assertEquals("101", sqlReadMan.getValueInCell("int_pk_table", "1", "int_col"));
        assertEquals("101", sqlReadMan.getValueInCell("int_pk_table", 1, "int_col"));
    }

    @Test
    void givenEmptyTablenameString_whenGettingValueInCell_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.getValueInCell("", "1", "int_col");
        });

        assertThrows(SQLException.class, () -> {
            sqlReadMan.getValueInCell("", 1, "int_col");
        });
    }

    @Test
    void givenTableNotExists_whenGettingValueInCell_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.getValueInCell("non_existant_table", "1", "int_col");
        });

        assertThrows(SQLException.class, () -> {
            sqlReadMan.getValueInCell("non_existant_table", 1, "int_col");
        });
    }

    @Test
    void givenEmptyPrimaryKeyString_whenGettingValueInCell_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.getValueInCell("str_pk_table", "", "int_col");
        });
    }

    @Test
    void givenPrimaryKeyNotExists_whenGettingValueInCell_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.getValueInCell("str_pk_table", "non_existant_pk", "int_col");
        });

        assertThrows(SQLException.class, () -> {
            sqlReadMan.getValueInCell("int_pk_table", 50, "int_col");
        });
    }

    @Test
    void givenEmptyColumnnameString_whenGettingValueInCell_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.getValueInCell("int_pk_table", "1", "");
        });

        assertThrows(SQLException.class, () -> {
            sqlReadMan.getValueInCell("int_pk_table", 1, "");
        });
    }

    @Test
    void givenColumnNotExists_whenGettingValueInCell_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.getValueInCell("int_pk_table", "1", "non_existant_col");
        });

        assertThrows(SQLException.class, () -> {
            sqlReadMan.getValueInCell("int_pk_table", 1, "non_existant_col");
        });
    }

    // whenRemovingFromTable

    @Test
    void givenTableAndPrimaryKeyExists_whenRemovingFromTable_thenSuccess() throws SQLException {
        sqlWriteMan.addNewRecordToTable("str_pk_table", "str_pk", "delete_this_row");
        assertTrue(sqlWriteMan.checkTableContainsPrimaryKey("str_pk_table", "delete_this_row"));
        sqlWriteMan.removeFromTable("str_pk_table", "delete_this_row");
        assertFalse(sqlWriteMan.checkTableContainsPrimaryKey("str_pk_table", "delete_this_row"));
    }

    @Test
    void givenEmptyTablenameString_whenRemovingFromTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.removeFromTable("", "1");
        });

        assertThrows(SQLException.class, () -> {
            sqlWriteMan.removeFromTable("", 1);
        });
    }

    @Test
    void givenTableNotExists_whenRemovingFromTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.removeFromTable("non_existant_table", "1");
        });

        assertThrows(SQLException.class, () -> {
            sqlWriteMan.removeFromTable("non_existant_table", 1);
        });
    }

    @Test
    void givenEmptyPrimaryKeyString_whenRemovingFromTable_thenDoesNothing() throws SQLException {
        sqlWriteMan.addBareTableToDB("remove_empty_pk_test_table", "int_pk");
        sqlWriteMan.addNewRecordToTable("remove_empty_pk_test_table", "int_pk", "1");

        sqlWriteMan.removeFromTable("remove_empty_pk_test_table", "");
        assertEquals(1, sqlWriteMan.getAllPrimaryKeys("remove_empty_pk_test_table").size());
    }

    @Test
    void givenPrimaryKeyNotExists_whenRemovingFromTable_thenDoesNothing() throws SQLException {
        sqlWriteMan.addBareTableToDB("remove_non_existant_pk_test_table", "int_pk");
        sqlWriteMan.addNewRecordToTable("remove_non_existant_pk_test_table", "int_pk", "1");

        sqlWriteMan.removeFromTable("remove_non_existant_pk_test_table", "5");
        assertEquals(1, sqlWriteMan.getAllPrimaryKeys("remove_non_existant_pk_test_table").size());
    }

    // whenGettingPrimaryKeyColumnName

    @Test
    void givenTableContainingRows_whenGettingPrimaryKeyColumnName_thenSuccess() throws SQLException {
        assertEquals("str_pk", sqlReadMan.getPrimaryKeyColumnName("str_pk_table"));
    }

    @Test
    void givenTableNotContainingRows_whenGettingPrimaryKeyColumnName_thenSuccess() throws SQLException {
        assertEquals("int_pk", sqlReadMan.getPrimaryKeyColumnName("no_rows_table"));
    }

    @Test
    void givenEmptyTablenameString_whenGettingPrimaryKeyColumnName_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.getPrimaryKeyColumnName("");
        });
    }

    @Test
    void givenTableNotExists_whenGettingPrimaryKeyColumnName_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.getPrimaryKeyColumnName("non_existant_table");
        });
    }

    // whenGettingAllPrimaryKeys

    @Test
    void givenTableContainingRows_whenGettingAllPrimaryKeys_thenSuccess() throws SQLException {
        List<String> allPrimaryKeys = sqlReadMan.getAllPrimaryKeys("int_pk_table");
        assertEquals(5, allPrimaryKeys.size());
        assertTrue(allPrimaryKeys.contains("1"));
        assertTrue(allPrimaryKeys.contains("2"));
        assertTrue(allPrimaryKeys.contains("3"));
        assertTrue(allPrimaryKeys.contains("4"));
        assertTrue(allPrimaryKeys.contains("5"));
    }

    @Test
    void givenTableNotContainingRows_whenGettingAllPrimaryKeys_thenReturnsEmptyList() throws SQLException {
        assertEquals(0, sqlReadMan.getAllPrimaryKeys("no_rows_table").size());
    }

    @Test
    void givenEmptyTablenameString_whenGettingAllPrimaryKeys_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.getAllPrimaryKeys("");
        });
    }

    @Test
    void givenTableNotExists_whenGettingAllPrimaryKeys_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.getAllPrimaryKeys("non_existant_table");
        });
    }

    // whenCheckingTableContainsPrimaryKey

    @Test
    void givenTableExistsAndPrimaryKeyNotExists_whenCheckingTableContainsPrimaryKey_thenReturnsFalse()
            throws SQLException {
        assertFalse(sqlReadMan.checkTableContainsPrimaryKey("int_pk_table", "70"));
    }

    @Test
    void givenTableAndPrimaryKeyExists_whenCheckingTableContainsPrimaryKey_thenReturnsTrue() throws SQLException {
        assertTrue(sqlReadMan.checkTableContainsPrimaryKey("int_pk_table", "4"));
    }

    @Test
    void givenEmptyTablenameString_whenCheckingTableContainsPrimaryKey_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.checkTableContainsPrimaryKey("", "1");
        });
    }

    @Test
    void givenTableNotExists_whenCheckingTableContainsPrimaryKey_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.checkTableContainsPrimaryKey("non_existant_table", "1");
        });
    }

    @Test
    void givenEmptyPrimaryKeyString_whenCheckingTableContainsPrimaryKey_thenReturnsFalse() throws SQLException {
        assertFalse(sqlReadMan.checkTableContainsPrimaryKey("int_pk_table", ""));
    }

    // whenCalcColumnLength

    @Test
    void givenTableContainingRows_whenCalcColumnLength_thenReturnsCorrectLength() throws SQLException {
        assertEquals(4, sqlReadMan.calcColumnLength("int_pk_table", "int_col"));
    }

    @Test
    void givenTableNotContainingRows_whenCalcColumnLength_thenReturn0() throws SQLException {
        assertEquals(0, sqlReadMan.calcColumnLength("no_rows_table", "int_col"));
    }

    @Test
    void givenEmptyTablenameString_whenCalcColumnLength_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.calcColumnLength("", "int_col");
        });
    }

    @Test
    void givenTableNotExists_whenCalcColumnLength_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.calcColumnLength("non_existant_table", "int_col");
        });
    }

    @Test
    void givenEmptyColumnnameString_whenCalcColumnLength_thenReturnsCountOfRows() throws SQLException {
        assertEquals(5, sqlReadMan.calcColumnLength("int_pk_table", ""));
    }

    @Test
    void givenColumnNotExists_whenCalcColumnLength_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.calcColumnLength("int_pk_table", "non_existant_pk");
        });
    }

    // whenCalcColumnLengthWithCondition

    @Test
    void givenTableContainingRows_whenCalcColumnLengthWithCondition_thenReturnsCorrectLength() throws SQLException {
        assertEquals(3, sqlReadMan.calcColumnLength("str_pk_table", "int_col", "str_pk!='pk5'"));
    }

    @Test
    void givenTableNotContainingRows_whenCalcColumnLengthWithCondition_thenReturn0() throws SQLException {
        assertEquals(0, sqlReadMan.calcColumnLength("no_rows_table", "int_col", "int_pk!=5"));
    }

    @Test
    void givenEmptyTablenameString_whenCalcColumnLengthWithCondition_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.calcColumnLength("", "int_col", "int_pk!=5");
        });
    }

    @Test
    void givenTableNotExists_whenCalcColumnLengthWithCondition_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.calcColumnLength("non_existant_table", "int_col", "int_pk!=5");
        });
    }

    @Test
    void givenEmptyColumnnameString_whenCalcColumnLengthWithCondition_thenReturnsCountOfRowsWithCondition()
            throws SQLException {
        assertEquals(4, sqlReadMan.calcColumnLength("int_pk_table", "", "int_pk!=5"));
    }

    @Test
    void givenColumnNotExists_whenCalcColumnLengthWithCondition_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.calcColumnLength("int_pk_table", "non_existant_pk", "int_pk!=5");
        });
    }

    @Test
    void givenEmptyConditionString_whenCalcColumnLengthWithCondition_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.calcColumnLength("str_pk_table", "int_col", "");
        });
    }

    @Test
    void givenInvalidConditionString_whenCalcColumnLengthWithCondition_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.calcColumnLength("str_pk_table", "int_col", "not a valid condition");
        });
    }

    // whenGettingMaxValueInColumn

    @Test
    void givenIntTypeColumnInTableContainingRows_whenGettingMaxValueInColumn_thenReturnsHighestValue()
            throws SQLException {
        assertEquals("101", sqlReadMan.getMaxValueInColumn("int_pk_table", "int_col"));
    }

    @Test
    void givenStringTypeColumnInTableContainingRows_whenGettingMaxValueInColumn_thenReturnsHighestValue()
            throws SQLException {
        assertEquals("ta", sqlReadMan.getMaxValueInColumn("int_pk_table", "str_col"));
    }

    @Test
    void givenAllValuesInColumnNull_whenGettingMaxValueInColumn_thenReturnsNull() throws SQLException {
        assertEquals(null, sqlReadMan.getMaxValueInColumn("int_pk_table", "all_null_col"));
    }

    @Test
    void givenTableNotContainingRows_whenGettingMaxValueInColumn_thenReturnsNull() throws SQLException {
        assertNull(sqlReadMan.getMaxValueInColumn("no_rows_table", "int_col"));
    }

    @Test
    void givenEmptyTablenameString_whenGettingMaxValueInColumn_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.getMaxValueInColumn("", "int_col");
        });
    }

    @Test
    void givenTableNotExists_whenGettingMaxValueInColumn_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.getMaxValueInColumn("non_existant_table", "int_col");
        });
    }

    @Test
    void givenEmptyColumnnameString_whenGettingMaxValueInColumn_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.getMaxValueInColumn("int_pk_table", "");
        });
    }

    @Test
    void givenColumnNotExists_whenGettingMaxValueInColumn_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlReadMan.getMaxValueInColumn("int_pk_table", "non_existant_col");
        });
    }

    // whenCheckingIsISOLocalDateFormat

    @Test
    void givenStringIsISOLocalDateFormat_whenCheckingIsISOLocalDateFormat_thenReturnsTrue() {
        assertTrue(SQLManager.isISOLocalDateFormat("2021-02-01"));
    }

    @Test
    void givenStringNotISOLocalDateFormat_whenCheckingIsISOLocalDateFormat_thenReturnsFalse() {
        assertFalse(SQLManager.isISOLocalDateFormat("not-iso-date"));
    }

    @Test
    void givenEmptyString_whenCheckingIsISOLocalDateFormat_thenReturnsFalse() {
        assertFalse(SQLManager.isISOLocalDateFormat(""));
    }

    // whenCheckingIsChronologicalOrder

    @Test
    void givenFirstDateBeforeSecondDate_whenCheckingIsChronologicalOrder_thenReturnsTrue() {
        assertTrue(SQLManager.isChronologicalOrder("2019-01-01", "2021-02-01"));
    }

    @Test
    void givenFirstDateAfterSecondDate_whenCheckingIsChronologicalOrder_thenReturnsFalse() {
        assertFalse(SQLManager.isChronologicalOrder("2021-03-01", "2019-01-02"));
    }

    @Test
    void givenFirstDateIdenticalToSecondDate_whenCheckingIsChronologicalOrder_thenReturnsTrue() {
        assertTrue(SQLManager.isChronologicalOrder("2021-02-01", "2021-02-01"));
    }

    @Test
    void givenEmptyDateString_whenCheckingIsChronologicalOrder_thenThrowsDateTimeParseException() {
        assertThrows(DateTimeParseException.class, () -> {
            SQLManager.isChronologicalOrder("", "2021-02-01");
        });

        assertThrows(DateTimeParseException.class, () -> {
            SQLManager.isChronologicalOrder("2021-02-01", "");
        });
    }

    @Test
    void givenInvalidDateString_whenCheckingIsChronologicalOrder_thenThrowsDateTimeParseException() {
        assertThrows(DateTimeParseException.class, () -> {
            SQLManager.isChronologicalOrder("01-01-2021", "2021-02-01");
        });

        assertThrows(DateTimeParseException.class, () -> {
            SQLManager.isChronologicalOrder("2022-02-01", "01-01-2021");
        });
    }

    // whenRenamingTable

    @Test
    void givenTableExistsAndNewNameNotExists_whenRenamingTable_thenSuccess() throws SQLException {
        sqlWriteMan.addBareTableToDB("table_to_rename", "int_pk");
        assertTrue(sqlWriteMan.checkTableExists("table_to_rename"));

        sqlWriteMan.renameTable("table_to_rename", "renamed_table");
        assertFalse(sqlWriteMan.checkTableExists("table_to_rename"));
        assertTrue(sqlWriteMan.checkTableExists("renamed_table"));
    }

    @Test
    void givenTableExistsAndNewNameAlreadyExists_whenRenamingTable_thenThrowsSQLException() throws SQLException {
        sqlWriteMan.addBareTableToDB("table_to_rename_new_name_exists", "int_pk");
        sqlWriteMan.addBareTableToDB("renamed_table_new_name_exists", "int_pk");
        assertTrue(sqlWriteMan.checkTableExists("table_to_rename_new_name_exists"));
        assertTrue(sqlWriteMan.checkTableExists("renamed_table_new_name_exists"));

        assertThrows(SQLException.class, () -> {
            sqlWriteMan.renameTable("table_to_rename_new_name_exists", "renamed_table_new_name_exists");
        });
    }

    @Test
    void givenEmptyNewNameString_whenRenamingTable_thenThrowsSQLException() throws SQLException {
        sqlWriteMan.addBareTableToDB("table_to_rename_empty_string", "int_pk");

        assertThrows(SQLException.class, () -> {
            sqlWriteMan.renameTable("table_to_rename_empty_string", "");
        });

        assertFalse(sqlWriteMan.checkTableExists(""));
    }

    @Test
    void givenTableNotExist_whenRenamingTable_thenThrowsSQLException() throws SQLException {
        assertFalse(sqlWriteMan.checkTableExists("non_existant_table_to_rename"));

        assertThrows(SQLException.class, () -> {
            sqlWriteMan.renameTable("non_existant_table_to_rename", "renamed_table_that_does_not_exists");
        });
    }

    @Test
    void givenEmptyTablenameString_whenRenamingTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.renameTable("", "renamed_table_empty_string");
        });
    }

    // whenRemovingTable

    @Test
    void givenTableExists_whenRemovingTable_thenSuccess() throws SQLException {
        sqlWriteMan.addBareTableToDB("table_to_remove", "int_pk");
        assertTrue(sqlWriteMan.checkTableExists("table_to_remove"));
        sqlWriteMan.removeTable("table_to_remove");
        assertFalse(sqlWriteMan.checkTableExists("table_to_remove"));
    }

    @Test
    void givenTableNotExists_whenRemovingTable_thenThrowsSQLException() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.removeTable("non_existant_table");
        });
    }

    @Test
    void givenEmptyTablenameString_whenRemovingTable_thenSuccess() {
        assertThrows(SQLException.class, () -> {
            sqlWriteMan.removeTable("");
        });
    }

}
