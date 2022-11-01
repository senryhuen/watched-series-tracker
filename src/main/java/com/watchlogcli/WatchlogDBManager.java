package com.watchlogcli;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.apimanager.TVMazeAPI;
import com.sqlmanager.SQLManager;

/**
 * A class containing only static methods for managing the main database (which
 * stores information about series from TVMaze and related statistics).
 * 
 * <p>
 * This class uses the <code>SQLManager</code> class to create or manipulate the
 * database in
 * specific ways (to maintain structure).
 * </p>
 * 
 * @author senryhuen
 */
public final class WatchlogDBManager {

    private static SQLManager sqlMan;

    /**
     * Starts a connection to the main database using <code>SQLManager</code>. A
     * connection needs to be started before anything can be done with the database.
     * 
     * <p>
     * Use {@link #closeWatchlogDB()} to close connection after it is no longer
     * needed.
     * </p>
     * 
     * @throws SQLException
     */
    public static void startWatchlogDB() throws SQLException {
        sqlMan = new SQLManager("src/main/java/com/watchlogcli/watchlog_info.db");

        try {
            createSeriesTrackingDB();
        } catch (DBRollbackOccurredException e) {
            String errorMessage = String.format("Something went wrong with the main database '%s'.", sqlMan.dbPath);
            throw new RuntimeException(errorMessage, e);
        }
    }

    /**
     * Same as {@link #startWatchlogDB()}, but intended to be used for testing.
     * 
     * @param useTestDB If <code>true</code>, then connects to a disposable database
     *                  intended for testing instead of main database
     * @throws SQLException
     */
    public static void startWatchlogDB(boolean useTestDB) throws SQLException {
        if (useTestDB) {
            sqlMan = new SQLManager("src/test/java/com/watchlogcli/testWatchlog.db");
        } else {
            sqlMan = new SQLManager("src/main/java/com/watchlogcli/watchlog_info.db");
        }

        try {
            createSeriesTrackingDB();
        } catch (DBRollbackOccurredException e) {
            String errorMessage = String.format("Something went wrong with the main database '%s'.", sqlMan.dbPath);
            throw new RuntimeException(errorMessage, e);
        }
    }

    /**
     * Closes connection to the main database.
     * 
     * <p>
     * Use {@link #startWatchlogDB()} to reconnect to the database.
     * </p>
     * 
     * @throws SQLException
     */
    public static void closeWatchlogDB() {
        try {
            sqlMan.closeConnection();
        } catch (Exception e) {
            throw new RuntimeException("Failed to close database", e);
        }
    }

    /**
     * Creates database and tables if they do not exist.
     * 
     * @param dbName
     * @throws SQLException                If creating / opening connection to
     *                                     database fails
     * @throws DBRollbackOccurredException If creating table fails (whilst adding
     *                                     columns, which may happen if the columns
     *                                     already exist)
     */
    public static void createSeriesTrackingDB() throws SQLException, DBRollbackOccurredException {
        try {
            if (!sqlMan.checkTableExists("series")) {
                createSeriesTable();
            }
            if (!sqlMan.checkTableExists("episode")) {
                createEpisodeTable();
            }
            if (!sqlMan.checkTableExists("series_watchlog")) {
                createSeriesWatchLogTable();
            }
            if (!sqlMan.checkTableExists("episode_watchlog")) {
                createEpisodeWatchLogTable();
            }
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Creates table: series (series_id, name, status, premiere_date, ended_date).
     * 
     * @throws SQLException
     * @throws DBRollbackOccurredException
     */
    private static void createSeriesTable() throws SQLException, DBRollbackOccurredException {
        sqlMan.startTransaction();

        try {
            sqlMan.addBareTableToDB("series", "series_id");
            sqlMan.addStringTypeColumnToTable("series", "name");
            sqlMan.addStringTypeColumnToTable("series", "status");
            sqlMan.addStringTypeColumnToTable("series", "premiere_date");
            sqlMan.addStringTypeColumnToTable("series", "ended_date");
        } catch (SQLException e) {
            sqlMan.rollbackTransaction();
            throw new DBRollbackOccurredException(
                    "Failed to create table 'series', any changes made were rolled back to its original state", e);
        }

        sqlMan.endTransaction();
    }

    /**
     * Creates table: episode (episode_id, series_id, season_num, episode_num, name,
     * airdate, runtime, alternate_episode_num).
     * 
     * @throws SQLException
     * @throws DBRollbackOccuredException
     */
    private static void createEpisodeTable() throws SQLException, DBRollbackOccurredException {
        sqlMan.startTransaction();

        try {
            String columns = "episode_id INT NOT NULL PRIMARY KEY, series_id INT, FOREIGN KEY(series_id) REFERENCES series(series_id)";
            sqlMan.addTableToDB("episode", columns);

            sqlMan.addStringTypeColumnToTable("episode", "season_num");
            sqlMan.addStringTypeColumnToTable("episode", "name");
            sqlMan.addStringTypeColumnToTable("episode", "episode_num");
            sqlMan.addStringTypeColumnToTable("episode", "airdate");
            sqlMan.addStringTypeColumnToTable("episode", "runtime");
            sqlMan.addStringTypeColumnToTable("episode", "alternate_episode_num");
        } catch (SQLException e) {
            sqlMan.rollbackTransaction();
            throw new DBRollbackOccurredException(
                    "Failed to create table 'episode', any changes made were rolled back to its original state", e);
        }

        sqlMan.endTransaction();
    }

    /**
     * Creates table: series_watchlog (series_watchlog_id, series_id, start_date,
     * finish_date, finished).
     * 
     * @throws SQLException
     * @throws DBRollbackOccurredException
     */
    private static void createSeriesWatchLogTable() throws SQLException, DBRollbackOccurredException {
        sqlMan.startTransaction();

        try {
            String columns = "series_watchlog_id INTEGER PRIMARY KEY AUTOINCREMENT, series_id INT NOT NULL, FOREIGN KEY(series_id) REFERENCES series(series_id)";
            sqlMan.addTableToDB("series_watchlog", columns);

            sqlMan.addStringTypeColumnToTable("series_watchlog", "start_date");
            sqlMan.addStringTypeColumnToTable("series_watchlog", "finish_date");
            sqlMan.addBooleanTypeColumnToTable("series_watchlog", "finished");
        } catch (SQLException e) {
            sqlMan.rollbackTransaction();
            throw new DBRollbackOccurredException(
                    "Failed to create table 'series_watchlog', any changes made were rolled back to its original state",
                    e);
        }

        sqlMan.endTransaction();
    }

    /**
     * Creates table: episode_watchlog (episode_watchlog_id, episode_id, start_date,
     * finish_date, finished).
     * 
     * @throws SQLException
     * @throws DBRollbackOccurredException
     */
    private static void createEpisodeWatchLogTable() throws SQLException, DBRollbackOccurredException {
        sqlMan.startTransaction();

        try {
            String columns = "episode_watchlog_id INTEGER PRIMARY KEY AUTOINCREMENT, episode_id INT NOT NULL, FOREIGN KEY(episode_id) REFERENCES episode(episode_id)";
            sqlMan.addTableToDB("episode_watchlog", columns);

            sqlMan.addStringTypeColumnToTable("episode_watchlog", "start_date");
            sqlMan.addStringTypeColumnToTable("episode_watchlog", "finish_date");
            sqlMan.addBooleanTypeColumnToTable("episode_watchlog", "finished");
        } catch (SQLException e) {
            sqlMan.rollbackTransaction();
            throw new DBRollbackOccurredException(
                    "Failed to create table 'episode_watchlog', any changes made were rolled back to its original state",
                    e);
        }

        sqlMan.endTransaction();
    }

    /**
     * Adds info of a series specified by <code>seriesID</code>, retrieved from
     * <code>TVMazeAPI</code>. This updates the tables: series, episode.
     * 
     * @param seriesID TVMaze ID (different to IMDb ID) of series to add to database
     * @throws IOException
     * @throws InterruptedException
     * @throws SQLException
     */
    public static void addSeriesToTracked(String seriesID) throws IOException, InterruptedException, SQLException {
        TVMazeAPI tvmazeConnector = new TVMazeAPI(seriesID);

        try {
            // add info about series as a whole
            addSeriesInfoToTrackedSeries(tvmazeConnector);

            // add info specific to each episode
            addEpisodeInfoToTrackedSeries(tvmazeConnector);
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Adds info from TVMaze API about a series to database.
     * 
     * @param tvmazeConnector Holds info from TVMaze about the series of interest
     *                        and its episodes
     * @throws SQLException
     */
    private static void addSeriesInfoToTrackedSeries(TVMazeAPI tvmazeConnector) throws SQLException {
        String seriesID = tvmazeConnector.getCurrentSeriesID();
        int seriesIDint = Integer.parseInt(seriesID);

        if (!sqlMan.checkTableContainsPrimaryKey("series", seriesID)) {
            sqlMan.addNewRecordToTable("series", "series_id", seriesID);
        }

        sqlMan.setValueInCell("series", seriesIDint, "name", tvmazeConnector.getSeriesName());
        sqlMan.setValueInCell("series", seriesIDint, "status", tvmazeConnector.getSeriesStatus());
        sqlMan.setValueInCell("series", seriesIDint, "premiere_date", tvmazeConnector.getSeriesPremiereDate());
        sqlMan.setValueInCell("series", seriesIDint, "ended_date", tvmazeConnector.getSeriesEndedDate());
    }

    /**
     * Adds info from TVMaze API about each episode in a series to database.
     * 
     * @param tvmazeConnector Holds info from TVMaze about the series of interest
     *                        and its episodes
     * @throws SQLException
     */
    private static void addEpisodeInfoToTrackedSeries(TVMazeAPI tvmazeConnector) throws SQLException {
        String seriesID = tvmazeConnector.getCurrentSeriesID();

        String episodeIDString;
        int episodeID;

        for (int i = 1; i <= tvmazeConnector.getNumEpisodes(); i++) {
            episodeIDString = tvmazeConnector.getEpisodeID(i);
            episodeID = Integer.parseInt(episodeIDString);

            if (!sqlMan.checkTableContainsPrimaryKey("episode", episodeIDString)) {
                sqlMan.addNewRecordToTable("episode", "episode_id", episodeIDString);
            }

            sqlMan.setValueInCell("episode", episodeID, "series_id", seriesID);
            sqlMan.setValueInCell("episode", episodeID, "season_num", tvmazeConnector.getEpisodeSeasonNum(i));
            sqlMan.setValueInCell("episode", episodeID, "episode_num", tvmazeConnector.getEpisodeNum(i));
            sqlMan.setValueInCell("episode", episodeID, "name", tvmazeConnector.getEpisodeName(i));
            sqlMan.setValueInCell("episode", episodeID, "airdate", tvmazeConnector.getEpisodeAirDate(i));
            sqlMan.setValueInCell("episode", episodeID, "runtime", tvmazeConnector.getEpisodeRuntime(i));
        }
    }

    /**
     * Adds a complete entry (marked as finished) to <code>series_watchlog</code>.
     * 
     * <p>
     * Allows a complete entry which does not have either a <code>start_Date</code>
     * or a <code>finish_date</code> to be added.
     * </p>
     * 
     * @param seriesID   <code>TVMazeID</code> of series to create complete record
     *                   in <code>series_watchlog</code> for
     * @param startDate  <code>start_date</code> in "YYYY-MM-DD" (ISO_LOCAL_DATE)
     *                   format
     * @param finishDate <code>finish_date</code> in "YYYY-MM-DD" (ISO_LOCAL_DATE)
     *                   format
     * @throws SQLException
     * @throws IOException
     * @throws InterruptedException
     * @throws DBRollbackOccurredException
     */
    public static void addCompleteSeriesWatchlog(String seriesID, String startDate, String finishDate)
            throws SQLException, IOException, InterruptedException, DBRollbackOccurredException {
        // check atleast one date is provided
        if (startDate.isEmpty() && finishDate.isEmpty()) {
            throw new IllegalArgumentException(
                    "String startDate, finishDate: both dates were empty, atleast one valid date must be provided");
        }

        // check date Strings are in correct format
        if (!startDate.isEmpty() && !SQLManager.isISOLocalDateFormat(startDate)) {
            throw new IllegalArgumentException("String startDate: not in the correct format of 'YYYY-MM-DD'");
        }
        if (!finishDate.isEmpty() && !SQLManager.isISOLocalDateFormat(finishDate)) {
            throw new IllegalArgumentException("String finishDate: not in the correct format of 'YYYY-MM-DD'");
        }

        // ensure series ID tracked
        if (!isTrackedSeries(seriesID)) {
            addSeriesToTracked(seriesID);
        }

        sqlMan.startTransaction();

        // close open record if there is one
        int primaryKeyValue = getIndexOfOpenSeriesWatchlog(seriesID, false);
        if (primaryKeyValue != -1) {
            markFinishedInSeriesWatchlog(primaryKeyValue);
        }

        // create new record
        if (!startDate.isEmpty()) {
            setStartDateInSeriesWatchlog(seriesID, startDate);
        }
        if (!finishDate.isEmpty()) {
            setFinishDateInSeriesWatchlog(seriesID, finishDate);
        } else {
            // mark as finished even if there is no finishDate
            int newPrimaryKeyValue = getIndexOfOpenSeriesWatchlog(seriesID, false);
            if (newPrimaryKeyValue == -1) {
                sqlMan.rollbackTransaction();
                throw new DBRollbackOccurredException(
                        "addCompleteSeriesWatchlog(String, String, String) failed, any changes made were rolled back to its original state");
            }
            markFinishedInSeriesWatchlog(newPrimaryKeyValue);
        }

        // reopen record that was closed if there was one
        if (primaryKeyValue != -1) {
            // check there are no other open records
            int test = getIndexOfOpenSeriesWatchlog(seriesID, false);
            if (test == -1) {
                try {
                    unmarkFinishedInSeriesWatchlog(primaryKeyValue);
                } catch (CannotPerformActionException e) {
                }
            }
        }

        sqlMan.endTransaction();
    }

    /**
     * Sets the <code>start_date</code> attribute for open record of
     * <code>seriesID</code> in <code>series_watchlog</code> table. If there is not
     * an open record, or the open record already has a value for the
     * <code>start_date</code> attribute, a new record is created/opened.
     * 
     * <p>
     * An open record is a record in watchlog that is not marked as finished.
     * </p>
     * 
     * @param seriesID <code>TVMazeID</code> of series of open record to set
     *                 <code>start_date</code> for
     * @param date     <code>start_date</code> in "YYYY-MM-DD" (ISO_LOCAL_DATE)
     *                 format
     * @throws SQLException
     * @throws IOException
     * @throws InterruptedException
     */
    public static void setStartDateInSeriesWatchlog(String seriesID, String date)
            throws SQLException, IOException, InterruptedException {
        // check date String is in correct format
        if (!SQLManager.isISOLocalDateFormat(date)) {
            throw new IllegalArgumentException("String date: not in the correct format of 'YYYY-MM-DD'");
        }

        // ensure series ID tracked
        if (!isTrackedSeries(seriesID)) {
            addSeriesToTracked(seriesID);
        }

        int primaryKeyValue = getIndexOfOpenSeriesWatchlog(seriesID, true);

        // check for existing value in start_date column
        String currentValue = sqlMan.getValueInCell("series_watchlog", primaryKeyValue, "start_date");
        if (currentValue != null) {
            // if dates are equal, records are the same so nothing needs to be done
            if (currentValue.equals(date)) {
                return;
            }

            // if dates are not equal, close previous record and start a new one
            markFinishedInSeriesWatchlog(primaryKeyValue);
            primaryKeyValue = getIndexOfOpenSeriesWatchlog(seriesID, true);
        }
        sqlMan.setValueInCell("series_watchlog", primaryKeyValue, "start_date", date);
    }

    /**
     * Sets the <code>finish_date</code> attribute for open record of
     * <code>seriesID</code> in <code>series_watchlog</code> table. If there is not
     * an open record, a new record is created/opened. A record is closed after it
     * has a <code>finish_date</code>.
     * 
     * <p>
     * An open record is a record in watchlog that is not marked as finished.
     * </p>
     * 
     * @param seriesID <code>TVMazeID</code> of series of open record to set
     *                 <code>finish_date</code> for
     * @param date     <code>finish_date</code> in "YYYY-MM-DD" (ISO_LOCAL_DATE)
     *                 format
     * @throws SQLException
     * @throws InterruptedException
     * @throws IOException
     */
    public static void setFinishDateInSeriesWatchlog(String seriesID, String date)
            throws SQLException, InterruptedException, IOException {
        // check date String is in correct format
        if (!SQLManager.isISOLocalDateFormat(date)) {
            throw new IllegalArgumentException("String date: not in the correct format of 'YYYY-MM-DD'");
        }

        // ensure series ID tracked
        if (!isTrackedSeries(seriesID)) {
            addSeriesToTracked(seriesID);
        }

        int primaryKeyValue = getIndexOfOpenSeriesWatchlog(seriesID, true);

        // check end_date comes after start_date
        String startDate = sqlMan.getValueInCell("series_watchlog", primaryKeyValue, "start_date");

        if (startDate != null && !SQLManager.isChronologicalOrder(startDate, date)) {
            String errorMessage = String.format(
                    "String date: finish date cannot be before start date of current open record (%s). Use addCompleteSeriesWatchlog() to add a new record with just the finish date.",
                    startDate);
            throw new IllegalArgumentException(errorMessage);
        }

        sqlMan.setValueInCell("series_watchlog", primaryKeyValue, "finish_date", date);
        markFinishedInSeriesWatchlog(primaryKeyValue);
    }

    /**
     * Deletes a row/entry in <code>series_watchlog</code> identified by
     * <code>series_watchlog_id</code>.
     * 
     * @param series_watchlog_id Primary key value of entry in
     *                           <code>series_watchlog</code> to delete
     * @throws SQLException
     */
    public static void removeFromSeriesWatchlog(String series_watchlog_id) throws SQLException {
        sqlMan.removeFromTable("series_watchlog", Integer.parseInt(series_watchlog_id));
    }

    /**
     * Close a record in <code>series_watchlog</code> table by setting value in
     * <code>finished</code> column to <code>true</code>.
     * 
     * @param primaryKeyValue Primary key of record to close in
     *                        <code>series_watchlog</code> table
     * @throws SQLException
     */
    private static void markFinishedInSeriesWatchlog(int primaryKeyValue) throws SQLException {
        sqlMan.setValueInCell("series_watchlog", primaryKeyValue, "finished", "1");
    }

    /**
     * Reopen a record in <code>series_watchlog</code> table by setting value in
     * <code>finished</code> column to <code>false</code>.
     * 
     * <p>
     * A record with a value in <code>finish_date</code> must be marked as finished,
     * so it cannot be changed.
     * </p>
     * 
     * @param primaryKeyValue Primary key of record to reopen in
     *                        <code>series_watchlog</code> table
     * @throws SQLException
     * @throws CannotPerformActionException
     */
    private static void unmarkFinishedInSeriesWatchlog(int primaryKeyValue)
            throws SQLException, CannotPerformActionException {
        String val = sqlMan.getValueInCell("series_watchlog", primaryKeyValue, "finish_date");
        if (val != null) {
            String errorMessage = String
                    .format("Cannot unmark finished for primaryKeyValue: %s, as it has a finish_date", primaryKeyValue);
            throw new CannotPerformActionException(errorMessage);
        }
        sqlMan.setValueInCell("series_watchlog", primaryKeyValue, "finished", "0");
    }

    /**
     * @return <code>ArrayList</code> of all Series IDs (which are TVMaze IDs) as
     *         <code>String</code>s
     * @throws SQLException
     */
    public static List<String> getAllSeriesIDs() throws SQLException {
        return sqlMan.getAllPrimaryKeys("series");
    }

    /**
     * @return <code>ArrayList</code> of all episode IDs (which are TVMaze IDs) as
     *         <code>String</code>s
     * @throws SQLException
     */
    public static List<String> getAllEpisodeIDs() throws SQLException {
        return sqlMan.getAllPrimaryKeys("episode");
    }

    /**
     * @param openRecordsOnly If <code>true</code>, <code>series_watchlog_id</code>s
     *                        of records that are marked as finished are ignored,
     *                        else all <code>series_watchlog_id</code>s are included
     * @return <code>ArrayList</code> of <code>series_watchlog_id</code>s as
     *         <code>String</code>s
     * @throws SQLException
     */
    public static List<String> getAllSeriesWatchlogIDs(boolean openRecordsOnly) throws SQLException {
        String sql;
        if (openRecordsOnly) {
            sql = "SELECT series_watchlog_id FROM series_watchlog WHERE finished=0;";
        } else {
            sql = "SELECT series_watchlog_id FROM series_watchlog;";
        }

        try (ResultSet result = sqlMan.getQueryResults(sql)) {
            List<String> allPrimaryKeys = new ArrayList<>();
            while (result.next()) {
                allPrimaryKeys.add(result.getString(1));
            }

            return allPrimaryKeys;
        }
    }

    /**
     * Checks if a series is in the <code>series</code> table.
     * 
     * @param seriesID <code>TVMazeID</code> of series to search for in
     *                 <code>series_watchlog</code> table - which is also the
     *                 primary key
     * @return <code>true</code> if series <code>seriesID</code> is in the
     *         <code>series</code>
     *         table, <code>false</code> if not
     * @throws SQLException
     */
    public static boolean isTrackedSeries(String seriesID) throws SQLException {
        String SQLcondition = String.format("series_id=%s", seriesID);
        int count = sqlMan.calcColumnLength("series", "series_id", SQLcondition);

        if (count == 0) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Checks <code>series_watchlog_id</code> is a primary key in
     * <code>series_watchlog</code>.
     * 
     * @param series_watchlog_id Primary key value to check for in
     *                           <code>series_watchlog</code> table
     * @return <code>true</code> if <code>series_watchlog_id</code> is a primary key
     *         of <code>series_watchlog</code>, <code>false</code> otherwise
     * @throws SQLException
     */
    public static boolean checkSeriesWatchlogIDExists(String series_watchlog_id) throws SQLException {
        return sqlMan.checkTableContainsPrimaryKey("series_watchlog", series_watchlog_id);
    }

    /**
     * Gets the <code>series_id</code> of an entry in <code>series_watchlog</code>.
     * 
     * @param series_watchlog_id Primary key value of entry of interest in
     *                           <code>series_watchlog</code>
     * @return <code>series_id</code> as a <code>String</code>
     * @throws SQLException
     */
    public static String getSeriesIDFromSeriesWatchlogID(String series_watchlog_id) throws SQLException {
        String sql = String.format("SELECT series_id FROM series_watchlog WHERE series_watchlog_id = %s",
                series_watchlog_id);
        return sqlMan.getQueryResults(sql).getString(1);
    }

    /**
     * Checks if an open record for series of <code>seriesID</code> exists in
     * <code>series_watchlog</code>.
     * 
     * <p>
     * An open record is a record in watchlog that is not marked as finished.
     * </p>
     * 
     * @param seriesID <code>TVMazeID</code> of series to check
     * @return <code>true</code> if an open record for <code>seriesID</code> exists,
     *         <code>false</code> if not
     * @throws SQLException
     */
    public static boolean existsOpenSeriesWatchlog(String seriesID) throws SQLException {
        String SQLcondition = String.format("series_id=%s AND finished=0;", seriesID);
        int count = sqlMan.calcColumnLength("series_watchlog", "series_watchlog_id", SQLcondition);

        if (count == 1) {
            return true;
        } else if (count == 0) {
            return false;
        } else {
            closeClashingOpenSeriesWatchlogs(seriesID);
            return true;
        }

    }

    /**
     * Closes all open records apart from the most recent for <code>seriesID</code>
     * in <code>series_watchlog</code> table, as there should only be at most one
     * open record for a given <code>seriesID</code>.
     * 
     * <p>
     * An open record is a record in watchlog that is not marked as finished.
     * </p>
     * 
     * @param seriesID <code>TVMazeID</code> of series to close clashing records for
     * @throws SQLException
     */
    private static void closeClashingOpenSeriesWatchlogs(String seriesID) throws SQLException {
        String SQLcondition = String.format("series_id=%s AND finished=0;", seriesID);
        int count = sqlMan.calcColumnLength("series_watchlog", "series_watchlog_id", SQLcondition);

        int series_watchlog_id;
        for (int i = 0; i < count - 1; i++) {
            series_watchlog_id = getIndexOfOpenSeriesWatchlog(seriesID, false);
            markFinishedInSeriesWatchlog(series_watchlog_id);
        }
    }

    /**
     * Gets the primary key of an open record in <code>series_watchlog</code>,
     * selected by <code>series_id</code>, with the option to create one if one does
     * not exist.
     * 
     * <p>
     * An open record is a record in watchlog that is not marked as finished.
     * </p>
     * 
     * @param seriesID <code>TVMazeID</code> which is the <code>series_id</code> to
     *                 find open record for
     * @param forceful If <code>true</code>, an index will always be returned. A new
     *                 open record
     *                 for <code>seriesID</code> will be created if one does not
     *                 already exist.
     * @return Primary key of open record (for <code>seriesID</code>) in
     *         <code>series_watchlog</code>, -1 if no open record for <code>seriesID
     *         exists</code>
     * @throws SQLException
     */
    private static int getIndexOfOpenSeriesWatchlog(String seriesID, boolean forceful) throws SQLException {
        if (!existsOpenSeriesWatchlog(seriesID)) {
            if (forceful) {
                sqlMan.addNewRecordToTable("series_watchlog", "series_id", seriesID);
            } else {
                return -1;
            }
        }

        String sql = String.format("SELECT series_watchlog_id FROM series_watchlog WHERE series_id=%s AND finished=0;",
                seriesID);
        return sqlMan.getQueryResults(sql).getInt(1);
    }

    /**
     * Gets the primary key of an open record in <code>series_watchlog</code>,
     * selected by <code>series_id</code>.
     * 
     * @param seriesID <code>TVMazeID</code> which is the <code>series_id</code> to
     *                 find open record for
     * @return Primary key of open record (for <code>seriesID</code>) in
     *         <code>series_watchlog</code>, -1 if no open record for <code>seriesID
     *         exists</code>
     * @throws SQLException
     */
    public static int getIndexOfOpenSeriesWatchlog(String seriesID) throws SQLException {
        return getIndexOfOpenSeriesWatchlog(seriesID, false);
    }

    /**
     * @param series_watchlog_id Primary key value of entry in
     *                           <code>series_watchlog</code> to get
     *                           <code>start_date</code> of.
     * @return <code>start_date</code> in <code>series_watchlog</code> at
     *         <code>series_watchlog_id</code>, may be empty
     * @throws SQLException
     */
    public static String getStartDateInSeriesWatchlog(String series_watchlog_id) throws SQLException {
        return sqlMan.getValueInCell("series_watchlog", series_watchlog_id, "start_date");
    }

    /**
     * @param series_watchlog_id Primary key value of entry in
     *                           <code>series_watchlog</code> to get
     *                           <code>finish_date</code> of.
     * @return <code>finish_date</code> in <code>series_watchlog</code> at
     *         <code>series_watchlog_id</code>, may be empty
     * @throws SQLException
     */
    public static String getFinishDateInSeriesWatchlog(String series_watchlog_id) throws SQLException {
        return sqlMan.getValueInCell("series_watchlog", series_watchlog_id, "finish_date");
    }

    /**
     * @param series_watchlog_id Primary key value of entry in
     *                           <code>series_watchlog</code> to get
     *                           <code>finished</code> status of.
     * @return <code>true</code> if entry at <code>series_watchlog_id</code> is
     *         marked as finished, <code>false</code> if not
     * @throws SQLException
     */
    public static boolean getFinishedStatusInSeriesWatchlog(String series_watchlog_id) throws SQLException {
        int finishedStatus = Integer.parseInt(sqlMan.getValueInCell("series_watchlog", series_watchlog_id, "finished"));

        if (finishedStatus == 1) {
            return true;
        } else if (finishedStatus == 0) {
            return false;
        } else {
            throw new RuntimeException(String.format(
                    "Invalid value found in database in column 'finished' in 'series_watchlog' table, at '%s'",
                    series_watchlog_id));
        }
    }

    /**
     * Checks if a record in <code>series_watchlog</code> has the same values as
     * defined by the parameters <code>seriesID</code>, <code>startDate</code>,
     * <code>finishDate</code> and <code>finished</code>.
     * 
     * @param seriesID   <code>TVMazeID</code> of series to create complete record
     *                   in <code>series_watchlog</code> for
     * @param startDate  <code>start_date</code> in "YYYY-MM-DD" (ISO_LOCAL_DATE)
     *                   format
     * @param finishDate <code>finish_date</code> in "YYYY-MM-DD" (ISO_LOCAL_DATE)
     *                   format
     * @param finished   <code>true</code> if record is marked as complete,
     *                   <code>false</code> otherwise
     * @return <code>true</code> if a record in <code>series_watchlog</code> has the
     *         same values as defined by the parameters
     * @throws SQLException
     */
    public static boolean checkIdenticalSeriesWatchlogExists(String seriesID, String startDate, String finishDate,
            boolean finished) throws SQLException {
        String finishedCondition;
        String startDateCondition;
        String finishDateCondition;

        if (finished) {
            finishedCondition = "1";
        } else {
            finishedCondition = "0";
        }

        if (startDate.isEmpty()) {
            startDateCondition = " IS NULL";
        } else {
            startDateCondition = String.format("='%s'", startDate);
        }

        if (finishDate.isEmpty()) {
            finishDateCondition = " IS NULL";
        } else {
            finishDateCondition = String.format("='%s'", finishDate);
        }

        String condition = String.format("series_id=%s AND finished=%s AND start_date%s AND finish_date%s", seriesID,
                finishedCondition, startDateCondition, finishDateCondition);

        if (1 >= sqlMan.calcColumnLength("series_watchlog", "series_watchlog_id", condition)) {
            return true;
        }

        return false;
    }

    /**
     * Prints a row (entry) from <code>series_watchlog</code> in a format that will
     * form a table when multiple rows are printed. Columns are (series_watchlog_id,
     * series_id, series_name, start_date, finish_date, finished).
     * 
     * @param series_watchlog_id Primary key value of row in
     *                           <code>series_watchlog</code> to print
     * @param printHeader        If <code>true</code>, a header with the column
     *                           names will be
     *                           printed before the row itself
     * @throws SQLException
     */
    public static void printRowOfSeriesWatchlog(int series_watchlog_id, boolean printHeader) throws SQLException {
        String seriesID = sqlMan.getValueInCell("series_watchlog", series_watchlog_id, "series_id");
        String seriesName = sqlMan.getValueInCell("series", Integer.parseInt(seriesID), "name");
        String startDate = sqlMan.getValueInCell("series_watchlog", series_watchlog_id, "start_date");
        String finishDate = sqlMan.getValueInCell("series_watchlog", series_watchlog_id, "finish_date");
        String finished = sqlMan.getValueInCell("series_watchlog", series_watchlog_id, "finished");

        if (printHeader) {
            System.out.printf("%20s|%15s|%20s|%11s|%11s|%8s\n", "series_watchlog_id", "series_id", "series_name",
                    "start_date", "finish_date", "finished");
            for (int i = 0; i < 90; i++) {
                System.out.print("-");
            }
            System.out.println();
        }

        System.out.printf("%20s|%15s|%20s|%11s|%11s|%8s\n", String.valueOf(series_watchlog_id), seriesID, seriesName,
                startDate, finishDate, finished);
    }

    /**
     * Prints a row in a format that will form a table containing the columns
     * (series_id, series_name).
     * 
     * @param seriesID    TVMaze ID of series to be printed
     * @param printHeader If <code>true</code>, a header with the column names will
     *                    be printed before the row itself
     * @throws SQLException
     */
    public static void printRowOfSeriesSimple(int seriesID, boolean printHeader) throws SQLException {
        String seriesName = sqlMan.getValueInCell("series", seriesID, "name");

        if (printHeader) {
            System.out.printf("%15s|%20s\n", "series_id", "series_name");
            for (int i = 0; i < 36; i++) {
                System.out.print("-");
            }
            System.out.println();
        }

        System.out.printf("%15s|%20s\n", String.valueOf(seriesID), seriesName);
    }

    /**
     * Prints a row (entry) from <code>episode_watchlog</code> in a format that will
     * form a table when multiple rows are printed. Columns are
     * (episode_watchlog_id, episode_id, series_name, season_num, episode_num,
     * episode_name, start_date, finish_date, finished).
     * 
     * @param episode_watchlog_id Primary key value of row in
     *                            <code>episode_watchlog</code> to print
     * @param printHeader         If <code>true</code>, a header with the column
     *                            names will be
     *                            printed before the row itself
     * @throws SQLException
     */
    public static void printRowOfEpisodeWatchlog(int episode_watchlog_id, boolean printHeader) throws SQLException {
        int episodeID = Integer.parseInt(sqlMan.getValueInCell("episode_watchlog", episode_watchlog_id, "episode_id"));
        String seriesID = sqlMan.getValueInCell("episode", episodeID, "series_id");
        String seriesName = sqlMan.getValueInCell("series", Integer.parseInt(seriesID), "name");
        String seasonNum = sqlMan.getValueInCell("episode", episodeID, "season_num");
        String episodeNum = sqlMan.getValueInCell("episode", episodeID, "episode_num");
        String episodeName = sqlMan.getValueInCell("episode", episodeID, "name");
        String startDate = sqlMan.getValueInCell("episode_watchlog", episode_watchlog_id, "start_date");
        String finishDate = sqlMan.getValueInCell("episode_watchlog", episode_watchlog_id, "finish_date");
        String finished = sqlMan.getValueInCell("episode_watchlog", episode_watchlog_id, "finished");

        if (printHeader) {
            System.out.printf("%20s|%15s|%20s|%11s|%11s|%20s|%11s|%11s|%8s\n", "episode_watchlog_id", "episode_id",
                    "series_name", "season_num", "episode_num", "episode_name", "start_date", "finish_date",
                    "finished");
            for (int i = 0; i < 135; i++) {
                System.out.print("-");
            }
            System.out.println();
        }

        System.out.printf("%20s|%15s|%20s|%11s|%11s|%20s|%11s|%11s|%8s\n", String.valueOf(episode_watchlog_id),
                String.valueOf(episodeID), seriesName, seasonNum, episodeNum, episodeName, startDate, finishDate,
                finished);
    }

    /**
     * Prints a row in a format that will form a table containing the columns
     * (episode_id, series_name, season_num, episode_num, episode_name).
     * 
     * @param episodeID   TVMaze ID of episode to be printed
     * @param printHeader If <code>true</code>, a header with the column names will
     *                    be printed before the row itself
     * @throws SQLException
     */
    public static void printRowOfEpisodeSimple(int episodeID, boolean printHeader) throws SQLException {
        String seriesID = sqlMan.getValueInCell("episode", episodeID, "series_id");
        String seriesName = sqlMan.getValueInCell("series", Integer.parseInt(seriesID), "name");
        String seasonNum = sqlMan.getValueInCell("episode", episodeID, "season_num");
        String episodeNum = sqlMan.getValueInCell("episode", episodeID, "episode_num");
        String episodeName = sqlMan.getValueInCell("episode", episodeID, "name");

        if (printHeader) {
            System.out.printf("%15s|%20s|%11s|%11s|%20s\n", "episode_id", "series_name", "season_num", "episode_num",
                    "episode_name");
            for (int i = 0; i < 81; i++) {
                System.out.print("-");
            }
            System.out.println();
        }

        System.out.printf("%15s|%20s|%11s|%11s|%20s\n", String.valueOf(episodeID), seriesName, seasonNum, episodeNum,
                episodeName);
    }

    /**
     * Prints a formatted table of all rows (entries) in either
     * <code>series_watchlog</code> (columns: series_watchlog_id, series_id,
     * series_name, start_date, finish_date) or <code>episode_watchlog</code>
     * (columns: episode_watchlog_id, episode_id, series_name, season_num,
     * episode_num, episode_name, start_date, finish_date).
     * 
     * @param episodeNotSeries If <code>true</code>, rows of
     *                         <code>episode_watchlog</code> will
     *                         be printed instead of <code>series_watchlog</code>
     * @throws SQLException
     */
    public static void printFullWatchlogTable(boolean episodeNotSeries) throws SQLException {
        String tableName = "series_watchlog";
        if (episodeNotSeries) {
            tableName = "episode_watchlog";
        }

        List<String> allPrimaryKeys = sqlMan.getAllPrimaryKeys(tableName);

        boolean printHeader = true;
        for (String primaryKey : allPrimaryKeys) {
            int primaryKeyInt = Integer.parseInt(primaryKey);

            if (episodeNotSeries) {
                printRowOfEpisodeWatchlog(primaryKeyInt, printHeader);
            } else {
                printRowOfSeriesWatchlog(primaryKeyInt, printHeader);
            }

            if (printHeader) {
                printHeader = false;
            }
        }
    }

    /**
     * Prints a formatted table of all rows (entries) in
     * <code>series_watchlog</code> that are incomplete.
     * 
     * @throws SQLException
     */
    public static void printOpenSeriesWatchlogTable() throws SQLException {
        List<String> allPrimaryKeys = getAllSeriesWatchlogIDs(true);

        boolean printHeader = true;
        for (String primaryKey : allPrimaryKeys) {
            int primaryKeyInt = Integer.parseInt(primaryKey);

            printRowOfSeriesWatchlog(primaryKeyInt, printHeader);

            if (printHeader) {
                printHeader = false;
            }
        }
    }

    /**
     * Prints a formatted table showing all series in local database with columns
     * (series_id, series_name).
     * 
     * @throws SQLException
     */
    public static void printSimpleTable(boolean episodeNotSeries) throws SQLException {
        List<String> allPrimaryKeys;
        if (episodeNotSeries) {
            allPrimaryKeys = getAllEpisodeIDs();
        } else {
            allPrimaryKeys = getAllSeriesIDs();
        }

        boolean printHeader = true;
        for (String primaryKey : allPrimaryKeys) {
            int primaryKeyInt = Integer.parseInt(primaryKey);
            if (episodeNotSeries) {
                printRowOfEpisodeSimple(primaryKeyInt, printHeader);
            } else {
                printRowOfSeriesSimple(primaryKeyInt, printHeader);
            }

            if (printHeader) {
                printHeader = false;
            }
        }
    }

}
