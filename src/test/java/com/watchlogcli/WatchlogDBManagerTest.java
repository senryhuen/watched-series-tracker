package com.watchlogcli;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.sqlmanager.SQLManager;

import static org.junit.jupiter.api.Assertions.*;

public class WatchlogDBManagerTest {

    static String testWatchlogDBPath = "src/test/java/com/watchlogcli/testWatchlog.db";
    static String testWatchlogDBTempPath = "src/test/java/com/watchlogcli/testWatchlogBackup.db";

    @Nested
    class TestsSharingDB {

        static SQLManager sqlMan;

        @BeforeAll
        static void setUp() throws SQLException, IOException {
            WatchlogDBManager.startWatchlogDB(true);
            List<String> allPrimaryKeys = WatchlogDBManager.getAllSeriesWatchlogIDs(false);
            WatchlogDBManager.closeWatchlogDB();

            // only deleting all rows in series_watchlog table to reduce time loading
            // series_info from API
            sqlMan = new SQLManager(testWatchlogDBPath);
            for (String key : allPrimaryKeys) {
                sqlMan.removeFromTable("series_watchlog", key);
            }
            sqlMan.closeConnection();

            WatchlogDBManager.startWatchlogDB(true);
        }

        @AfterAll
        static void tearDown() throws SQLException {
            WatchlogDBManager.closeWatchlogDB();
        }

        // whenAddingSeriesToTracked

        @Test
        void givenEmptySeriesIDString_whenAddingSeriesToTracked_thenThrowsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class, () -> {
                WatchlogDBManager.addSeriesToTracked("");
            });
        }

        @Test
        void givenInvalidSeriesID_whenAddingSeriesToTracked_thenThrowsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class, () -> {
                WatchlogDBManager.addSeriesToTracked("notAValidId");
            });
        }

        // whenSettingStartDateInWatchlog

        @Test
        void givenNotExistsRecordWithSameSeriesId_whenSettingStartDateInWatchlog_thenSuccess()
                throws SQLException, IOException, InterruptedException {
            WatchlogDBManager.setStartDateInSeriesWatchlog("3", "2021-02-01");
            assertTrue(WatchlogDBManager.existsOpenSeriesWatchlog("3"));
            assertEquals("2021-02-01", WatchlogDBManager
                    .getStartDateInSeriesWatchlog(String.valueOf(WatchlogDBManager.getIndexOfOpenSeriesWatchlog("3"))));
        }

        @Test
        void givenExistsClosedRecordWithSameSeriesID_whenSettingStartDateInWatchlog_thenSuccess()
                throws SQLException, IOException, InterruptedException, DBRollbackOccurredException {
            WatchlogDBManager.addCompleteSeriesWatchlog("4", "2021-02-01", "");
            WatchlogDBManager.setStartDateInSeriesWatchlog("4", "2021-02-01");
            assertTrue(WatchlogDBManager.existsOpenSeriesWatchlog("4"));
            assertEquals("2021-02-01", WatchlogDBManager
                    .getStartDateInSeriesWatchlog(String.valueOf(WatchlogDBManager.getIndexOfOpenSeriesWatchlog("4"))));
        }

        @Test
        void givenExistsOpenRecordWithSameSeriesIDAndDate_whenSettingStartDateInWatchlog_thenDoesNothing()
                throws SQLException, IOException, InterruptedException {
            WatchlogDBManager.setStartDateInSeriesWatchlog("5", "2021-02-01");
            WatchlogDBManager.setStartDateInSeriesWatchlog("5", "2021-02-01");
            assertTrue(WatchlogDBManager.existsOpenSeriesWatchlog("5"));
        }

        @Test
        void givenExistsOpenRecordWithSameSeriesIDAndDifferentDate_whenSettingStartDateInWatchlog_thenClosesOtherRecordAndStartsNewRecord()
                throws SQLException, IOException, InterruptedException {
            WatchlogDBManager.setStartDateInSeriesWatchlog("6", "2021-01-01");
            WatchlogDBManager.setStartDateInSeriesWatchlog("6", "2021-02-01");
            assertTrue(WatchlogDBManager.existsOpenSeriesWatchlog("6"));

            assertEquals("2021-02-01", WatchlogDBManager
                    .getStartDateInSeriesWatchlog(String.valueOf(WatchlogDBManager.getIndexOfOpenSeriesWatchlog("6"))));

        }

        @Test
        void givenInvalidSeriesID_whenSettingStartDateInWatchlog_thenThrowsSQLException() {
            assertThrows(SQLException.class, () -> {
                WatchlogDBManager.setStartDateInSeriesWatchlog("notAValidId", "2021-02-01");
            });
        }

        @Test
        void givenEmptySeriesIDString_whenSettingStartDateInWatchlog_thenThrowsSQLException() {
            assertThrows(SQLException.class, () -> {
                WatchlogDBManager.setStartDateInSeriesWatchlog("", "2021-02-01");
            });
        }

        @Test
        void givenInvalidDate_whenSettingStartDateInWatchlog_thenThrowsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class, () -> {
                WatchlogDBManager.setStartDateInSeriesWatchlog("7", "notAValidDate");
            });
        }

        @Test
        void givenEmptyDateString_whenSettingStartDateInWatchlog_thenThrowsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class, () -> {
                WatchlogDBManager.setStartDateInSeriesWatchlog("7", "");
            });
        }

        // whenSettingFinishDateInWatchlog

        @Test
        void givenExistsOpenRecordWithSameSeriesID_whenSettingFinishDateInWatchlog_thenSuccess()
                throws SQLException, IOException, InterruptedException {
            WatchlogDBManager.setStartDateInSeriesWatchlog("11", "2021-02-01");

            int seriesWatchlogID = WatchlogDBManager.getIndexOfOpenSeriesWatchlog("11");
            assertNotEquals(-1, seriesWatchlogID);

            WatchlogDBManager.setFinishDateInSeriesWatchlog("11", "2021-02-02");

            assertTrue(WatchlogDBManager.getFinishedStatusInSeriesWatchlog(String.valueOf(seriesWatchlogID)));
            assertEquals("2021-02-02",
                    WatchlogDBManager.getFinishDateInSeriesWatchlog(String.valueOf(seriesWatchlogID)));
            assertEquals("2021-02-01",
                    WatchlogDBManager.getStartDateInSeriesWatchlog(String.valueOf(seriesWatchlogID)));
        }

        @Test
        void givenNotExistsOpenRecordWithSameSeriesID_whenSettingFinishDateInWatchlog_thenCreatedNewCompleteRecord()
                throws SQLException, IOException, InterruptedException {
            assertEquals(-1, WatchlogDBManager.getIndexOfOpenSeriesWatchlog("10"));

            WatchlogDBManager.setFinishDateInSeriesWatchlog("10", "2021-02-01");

            assertTrue(WatchlogDBManager.checkIdenticalSeriesWatchlogExists("10", "", "2021-02-01", true));
        }

        @Test
        void givenFinishDateBeforeStartDateOfOpenRecord_whenSettingFinishDateInWatchlog_thenThrowsIllegalArgumentException()
                throws SQLException, IOException, InterruptedException {
            WatchlogDBManager.setStartDateInSeriesWatchlog("8", "2021-02-01");
            assertThrows(IllegalArgumentException.class, () -> {
                WatchlogDBManager.setFinishDateInSeriesWatchlog("8", "2021-01-01");
            });
        }

        @Test
        void givenInvalidSeriesID_whenSettingFinishDateInWatchlog_thenThrowsSQLException() {
            assertThrows(SQLException.class, () -> {
                WatchlogDBManager.setFinishDateInSeriesWatchlog("notAValidID", "2021-02-01");
            });
        }

        void givenEmptySeriesIDString_whenSettingFinishDateInWatchlog_thenThrowsSQLException() {
            assertThrows(SQLException.class, () -> {
                WatchlogDBManager.setFinishDateInSeriesWatchlog("", "2021-02-01");
            });
        }

        void givenInvalidDate_whenSettingFinishDateInWatchlog_thenThrowsSQLException() {
            assertThrows(SQLException.class, () -> {
                WatchlogDBManager.setFinishDateInSeriesWatchlog("9", "notAValidDate");
            });
        }

        void givenEmptyDateString_whenSettingFinishDateInWatchlog_thenThrowsSQLException() {
            assertThrows(SQLException.class, () -> {
                WatchlogDBManager.setFinishDateInSeriesWatchlog("9", "");
            });
        }

        // whenAddingCompleteSeriesWatchlog

        @Test
        void givenNotExistsOpenRecordWithSameSeriesID_whenAddingCompleteSeriesWatchlog_thenSuccess()
                throws SQLException, IOException, InterruptedException, DBRollbackOccurredException {
            assertFalse(WatchlogDBManager.existsOpenSeriesWatchlog("20"));
            WatchlogDBManager.addCompleteSeriesWatchlog("20", "2021-02-01", "2021-02-02");

            assertTrue(WatchlogDBManager.checkIdenticalSeriesWatchlogExists("20", "2021-02-01", "2021-02-02", true));
        }

        @Test
        // should add a new complete record (marked as finished), and leave the
        // pre-existing open record unaltered
        void givenExistsOpenRecordWithSameSeriesID_whenAddingCompleteSeriesWatchlog_thenSuccess()
                throws SQLException, IOException, InterruptedException, DBRollbackOccurredException {
            WatchlogDBManager.setStartDateInSeriesWatchlog("21", "2021-02-01");

            int seriesWatchlogID = WatchlogDBManager.getIndexOfOpenSeriesWatchlog("21");

            WatchlogDBManager.addCompleteSeriesWatchlog("21", "2021-03-01", "2021-03-02");

            // check pre-existing open record unchanged
            assertFalse(WatchlogDBManager.getFinishedStatusInSeriesWatchlog(String.valueOf(seriesWatchlogID)));
            assertEquals("2021-02-01",
                    WatchlogDBManager.getStartDateInSeriesWatchlog(String.valueOf(seriesWatchlogID)));

            // check complete record added
            assertTrue(WatchlogDBManager.checkIdenticalSeriesWatchlogExists("21", "2021-03-01", "2021-03-02", true));
        }

        @Test
        void givenEmptyStartDateStringAndValidFinishDateString_whenAddingCompleteSeriesWatchlog_thenSuccess()
                throws SQLException, IOException, InterruptedException, DBRollbackOccurredException {
            assertFalse(WatchlogDBManager.existsOpenSeriesWatchlog("22"));
            WatchlogDBManager.addCompleteSeriesWatchlog("22", "", "2021-02-02");

            assertTrue(WatchlogDBManager.checkIdenticalSeriesWatchlogExists("22", "", "2021-02-02", true));
        }

        @Test
        void givenEmptyFinishDateStringAndValidStartDateString_whenAddingCompleteSeriesWatchlog_thenSuccess()
                throws SQLException, IOException, InterruptedException, DBRollbackOccurredException {
            assertFalse(WatchlogDBManager.existsOpenSeriesWatchlog("23"));
            WatchlogDBManager.addCompleteSeriesWatchlog("23", "2021-02-01", "");

            assertTrue(WatchlogDBManager.checkIdenticalSeriesWatchlogExists("23", "2021-02-01", "", true));
        }

        @Test
        void givenBothEmptyStartAndFinishDateString_whenAddingCompleteSeriesWatchlog_thenThrowsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class, () -> {
                WatchlogDBManager.addCompleteSeriesWatchlog("24", "", "");
            });
        }

        @Test
        void givenEmptySeriesIDString_whenAddingCompleteSeriesWatchlog_thenThrowsSQLException() {
            assertThrows(SQLException.class, () -> {
                WatchlogDBManager.addCompleteSeriesWatchlog("", "2021-02-01", "2021-02-02");
            });
        }

        @Test
        void givenInvalidSeriesID_whenAddingCompleteSeriesWatchlog_thenThrowsSQLException() {
            assertThrows(SQLException.class, () -> {
                WatchlogDBManager.addCompleteSeriesWatchlog("notAValidId", "2021-02-01", "2021-02-02");
            });
        }

        @Test
        void givenInvalidStartDate_whenAddingCompleteSeriesWatchlog_thenThrowsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class, () -> {
                WatchlogDBManager.addCompleteSeriesWatchlog("25", "notAValidDate", "2021-02-02");
            });
        }

        @Test
        void givenInvalidFinishDate_whenAddingCompleteSeriesWatchlog_thenThrowsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class, () -> {
                WatchlogDBManager.addCompleteSeriesWatchlog("26", "2021-02-01", "2021-021-01");
            });
        }

        // whenRemovingWatchlogFromTable

        @Test
        void givenValidSeriesWatchlogID_whenRemovingWatchlogFromTable_thenSuccess()
                throws SQLException, IOException, InterruptedException {
            assertFalse(WatchlogDBManager.existsOpenSeriesWatchlog("100"));
            WatchlogDBManager.setStartDateInSeriesWatchlog("100", "2021-02-01");
            assertTrue(WatchlogDBManager.existsOpenSeriesWatchlog("100"));
            WatchlogDBManager
                    .removeFromSeriesWatchlog(String.valueOf(WatchlogDBManager.getIndexOfOpenSeriesWatchlog("100")));
            assertFalse(WatchlogDBManager.existsOpenSeriesWatchlog("100"));
        }

        // whenGettingSeriesIDFromSeriesWatchlogID

        @Test
        void givenValidSeriesID_whenGettingSeriesIDFromSeriesWatchlogID_thenSuccess()
                throws SQLException, IOException, InterruptedException {
            WatchlogDBManager.setStartDateInSeriesWatchlog("27", "2021-02-01");
            String testSeriesID = String.valueOf(WatchlogDBManager.getIndexOfOpenSeriesWatchlog("27"));
            assertEquals("27", WatchlogDBManager.getSeriesIDFromSeriesWatchlogID(testSeriesID));
        }

        @Test
        void givenInvalidSeriesID_whenGettingSeriesIDFromSeriesWatchlogID_thenThrowsSQLException() {
            assertThrows(SQLException.class, () -> {
                WatchlogDBManager.getIndexOfOpenSeriesWatchlog("notAValidID");
            });
        }

        // whenGettingStartDateInSeriesWatchlog

        @Test
        void givenValidIDAndExistsValueInStartDate_whenGettingStartDateInSeriesWatchlog_thenSuccess()
                throws SQLException, IOException, InterruptedException {
            WatchlogDBManager.setStartDateInSeriesWatchlog("28", "2021-02-01");
            assertEquals("2021-02-01", WatchlogDBManager
                    .getStartDateInSeriesWatchlog(
                            String.valueOf(WatchlogDBManager.getIndexOfOpenSeriesWatchlog("28"))));
        }

        void givenValidIDAndNotExistsValueInStartDate_whenGettingStartDateInSeriesWatchlog_thenReturnsEmptyString() {

        }

        @Test
        void givenInvalidID_whenGettingStartDateInSeriesWatchlog_thenThrowsSQLException() {
            assertThrows(SQLException.class, () -> {
                WatchlogDBManager.getStartDateInSeriesWatchlog("notAValidID");
            });
        }

        @Test
        void givenEmptyIDString_whenGettingStartDateInSeriesWatchlog_thenThrowsSQLException() {
            assertThrows(SQLException.class, () -> {
                WatchlogDBManager.getStartDateInSeriesWatchlog("");
            });
        }

        // whenGettingFinishDateInSeriesWatchlog

        @Test
        void givenValidIDAndExistsValueInFinishDate_whenGettingFinishDateInSeriesWatchlog_thenSuccess()
                throws SQLException, IOException, InterruptedException {
            WatchlogDBManager.setStartDateInSeriesWatchlog("29", "2021-02-01");
            String seriesWatchlogID = String.valueOf(WatchlogDBManager.getIndexOfOpenSeriesWatchlog("29"));
            WatchlogDBManager.setFinishDateInSeriesWatchlog("29", "2021-02-02");
            assertEquals("2021-02-02", WatchlogDBManager.getFinishDateInSeriesWatchlog(seriesWatchlogID));
        }

        @Test
        void givenValidIDAndNotExistsValueInFinishDate_whenGettingFinishDateInSeriesWatchlog_thenReturnsEmptyString()
                throws SQLException, IOException, InterruptedException {
            WatchlogDBManager.setStartDateInSeriesWatchlog("30", "2021-02-01");

            assertNull(WatchlogDBManager
                    .getFinishDateInSeriesWatchlog(
                            String.valueOf(WatchlogDBManager.getIndexOfOpenSeriesWatchlog("30"))));
        }

        @Test
        void givenEmptyIDString_whenGettingFinishDateInSeriesWatchlog_thenThrowsSQLException() {
            assertThrows(SQLException.class, () -> {
                WatchlogDBManager.getFinishDateInSeriesWatchlog("");
            });
        }

        @Test
        void givenInvalidID_whenGettingFinishDateInSeriesWatchlog_thenThrowsSQLException() {
            assertThrows(SQLException.class, () -> {
                WatchlogDBManager.getFinishDateInSeriesWatchlog("notAValidID");
            });
        }

        // whenGettingFinishedStatusInSeriesWatchlog

        @Test
        void givenValidIDAndRecordFinished_whenGettingFinishedStatusInSeriesWatchlog_thenReturnsTrue()
                throws SQLException, IOException, InterruptedException {
            WatchlogDBManager.setStartDateInSeriesWatchlog("31", "2021-02-01");
            String seriesWatchlogID = String.valueOf(WatchlogDBManager.getIndexOfOpenSeriesWatchlog("31"));
            WatchlogDBManager.setFinishDateInSeriesWatchlog("31", "2021-02-02");
            assertTrue(WatchlogDBManager.getFinishedStatusInSeriesWatchlog(seriesWatchlogID));
        }

        @Test
        void givenValidIDAndRecordNotFinished_whenGettingFinishedStatusInSeriesWatchlog_thenReturnsFalse()
                throws SQLException, IOException, InterruptedException {
            WatchlogDBManager.setStartDateInSeriesWatchlog("32", "2021-02-01");

            assertFalse(WatchlogDBManager.getFinishedStatusInSeriesWatchlog(
                    String.valueOf(WatchlogDBManager.getIndexOfOpenSeriesWatchlog("32"))));
        }

        @Test
        void givenEmptyIDString_whenGettingFinishedStatusInSeriesWatchlog_thenThrowsSQLException() {
            assertThrows(SQLException.class, () -> {
                WatchlogDBManager.getFinishedStatusInSeriesWatchlog("");
            });
        }

        @Test
        void givenInvalidID_whenGettingFinishedStatusInSeriesWatchlog_thenThrowsSQLException() {
            assertThrows(SQLException.class, () -> {
                WatchlogDBManager.getFinishedStatusInSeriesWatchlog("notAValidID");
            });
        }

    }

    @Nested
    class TestsRequiringFreshDB {
        static SQLManager sqlMan;

        static List<String> requiredTablenames = new ArrayList<>();

        @BeforeAll
        static void setUp() throws SQLException, IOException {
            requiredTablenames.add("series");
            requiredTablenames.add("episode");
            requiredTablenames.add("series_watchlog");
            requiredTablenames.add("episode_watchlog");

            File mainTestDBFile = new File(testWatchlogDBPath);
            File backupTestDBFile = new File(testWatchlogDBTempPath);

            Files.deleteIfExists(backupTestDBFile.toPath());
            if (!mainTestDBFile.renameTo(backupTestDBFile)) {
                throw new RuntimeException(String.format("Failed to rename file"));
            }
        }

        @BeforeEach
        void clearDB() throws IOException {
            File mainTestDBFile = new File(testWatchlogDBPath);
            Files.deleteIfExists(mainTestDBFile.toPath());
        }

        @AfterAll
        static void tearDown() throws SQLException, IOException {
            File mainTestDBFile = new File(testWatchlogDBPath);
            File backupTestDBFile = new File(testWatchlogDBTempPath);

            Files.deleteIfExists(mainTestDBFile.toPath());
            if (!backupTestDBFile.renameTo(mainTestDBFile)) {
                throw new RuntimeException(String.format("Failed to rename file"));
            }
        }

        // whenCreatingSeriesTrackingDB

        @Test
        void givenDBContainsNoTables_whenCreatingSeriesTrackingDB_thenCreatesRequiredTables() {
            try {
                WatchlogDBManager.startWatchlogDB(true);
                WatchlogDBManager.closeWatchlogDB();

                sqlMan = new SQLManager(testWatchlogDBPath);
                List<String> allTablenames = sqlMan.getAllTables();
                for (String tablename : requiredTablenames) {
                    assertTrue(allTablenames.contains(tablename));
                }
                sqlMan.closeConnection();
            } catch (Exception e) {
                fail();
            }
        }

        @Test
        void givenDBAlreadyContainsSomeRequiredTables_whenCreatingSeriesTrackingDB_thenCreatesMissingTables() {
            try {
                WatchlogDBManager.startWatchlogDB(true);
                WatchlogDBManager.closeWatchlogDB();

                sqlMan = new SQLManager(testWatchlogDBPath);
                sqlMan.removeTable("series_watchlog");
                sqlMan.closeConnection();

                WatchlogDBManager.startWatchlogDB(true);
                WatchlogDBManager.closeWatchlogDB();

                sqlMan = new SQLManager(testWatchlogDBPath);
                List<String> allTablenames = sqlMan.getAllTables();
                for (String tablename : requiredTablenames) {
                    assertTrue(allTablenames.contains(tablename));
                }
                sqlMan.closeConnection();
            } catch (Exception e) {
                fail();
            }
        }

        @Test
        void givenDBAlreadyContainsAllRequiredTables_whenCreatingSeriesTrackingDB_thenDoesNothing() {
            try {
                WatchlogDBManager.startWatchlogDB(true);
                WatchlogDBManager.closeWatchlogDB();

                WatchlogDBManager.startWatchlogDB(true);
                WatchlogDBManager.closeWatchlogDB();

                sqlMan = new SQLManager(testWatchlogDBPath);
                List<String> allTablenames = sqlMan.getAllTables();
                for (String tablename : requiredTablenames) {
                    assertTrue(allTablenames.contains(tablename));
                }
                sqlMan.closeConnection();
            } catch (Exception e) {
                fail();
            }
        }

        void givenDBAlreadyContainsAllRequiredTablesButMissingColumns_whenCreatingSeriesTrackingDB_thenAddsMissingColumns() {
            // // currently does not handle but should (currently does not assert columns
            // exists, adding columns already tested separately though)
        }

        // whenAddingSeriesToTracked

        @Test
        void givenValidSeriesID_whenAddingSeriesToTracked_thenSuccess()
                throws SQLException, IOException, InterruptedException {
            try {
                WatchlogDBManager.startWatchlogDB(true);
                WatchlogDBManager.addSeriesToTracked("435"); // TVMazeID for "Smallville"
                WatchlogDBManager.closeWatchlogDB();

                sqlMan = new SQLManager(testWatchlogDBPath);

                assertEquals("Smallville", sqlMan.getValueInCell("series", "435", "name"));
                assertEquals("Ended", sqlMan.getValueInCell("series", "435", "status"));
                assertEquals("2001-10-16", sqlMan.getValueInCell("series", "435", "premiere_date"));
                assertEquals("2011-05-13", sqlMan.getValueInCell("series", "435", "ended_date"));

                assertEquals("435", sqlMan.getValueInCell("episode", "41130", "series_id"));
                assertEquals("1", sqlMan.getValueInCell("episode", "41130", "season_num"));
                assertEquals("Metamorphosis", sqlMan.getValueInCell("episode", "41130", "name"));
                assertEquals("2", sqlMan.getValueInCell("episode", "41130", "episode_num"));
                assertEquals("2001-10-23", sqlMan.getValueInCell("episode", "41130", "airdate"));
                assertEquals("60", sqlMan.getValueInCell("episode", "41130", "runtime"));
                assertNull(sqlMan.getValueInCell("episode", "41130", "alternate_episode_num"));

                sqlMan.closeConnection();
            } catch (Exception e) {
                fail();
            }
        }

        // whenGettingAllSeriesIDs

        @Test
        void givenSeriesIDsExists_whenGettingAllSeriesIDs_thenReturnsListContainingAllSeriesIDs() {
            try {
                WatchlogDBManager.startWatchlogDB(true);
                WatchlogDBManager.addSeriesToTracked("435"); // TVMazeID for "Smallville"
                WatchlogDBManager.addSeriesToTracked("180"); // TVMazeID for "Firefly"
                List<String> allSeriesIDs = WatchlogDBManager.getAllSeriesIDs();
                WatchlogDBManager.closeWatchlogDB();

                assertEquals(2, allSeriesIDs.size());
                assertTrue(allSeriesIDs.contains("435"));
                assertTrue(allSeriesIDs.contains("180"));
            } catch (Exception e) {
                fail();
            }
        }

        // whenGettingAllEpisodeIDs

        @Test
        void givenEpisodeIDsExists_whenGettingAllEpisodeIDs_thenReturnsListContainingAllEpisodeIDs() {
            try {
                WatchlogDBManager.startWatchlogDB(true);
                WatchlogDBManager.addSeriesToTracked("180"); // TVMazeID for "Firefly"
                List<String> allEpisodeIDs = WatchlogDBManager.getAllEpisodeIDs();
                WatchlogDBManager.closeWatchlogDB();

                assertEquals(14, allEpisodeIDs.size());
                assertTrue(allEpisodeIDs.contains("12995"));
                assertTrue(allEpisodeIDs.contains("12996"));
                assertTrue(allEpisodeIDs.contains("12997"));
                assertTrue(allEpisodeIDs.contains("12998"));
                assertTrue(allEpisodeIDs.contains("12999"));
                assertTrue(allEpisodeIDs.contains("13000"));
                assertTrue(allEpisodeIDs.contains("13001"));
                assertTrue(allEpisodeIDs.contains("13002"));
                assertTrue(allEpisodeIDs.contains("13003"));
                assertTrue(allEpisodeIDs.contains("13004"));
                assertTrue(allEpisodeIDs.contains("13005"));
                assertTrue(allEpisodeIDs.contains("13007"));
                assertTrue(allEpisodeIDs.contains("13008"));
                assertTrue(allEpisodeIDs.contains("13009"));
            } catch (Exception e) {
                fail();
            }
        }

        // whenGettingAllSeriesWatchlogIDs

        @Test
        void givenRecordsExistAndNotOpenRecordsOnly_whenGettingAllSeriesWatchlogIDs_thenReturnsListContainingAllSeriesWatchlogIDs() {
            try {
                WatchlogDBManager.startWatchlogDB(true);

                WatchlogDBManager.addCompleteSeriesWatchlog("435", "2021-02-01", "2021-02-02");
                WatchlogDBManager.setStartDateInSeriesWatchlog("180", "2021-03-01");
                List<String> allSeriesWatchlogIDs = WatchlogDBManager.getAllSeriesWatchlogIDs(false);

                WatchlogDBManager.closeWatchlogDB();

                assertEquals(2, allSeriesWatchlogIDs.size());
                assertTrue(allSeriesWatchlogIDs.contains("1"));
                assertTrue(allSeriesWatchlogIDs.contains("2"));

            } catch (Exception e) {
                fail();
            }
        }

        @Test
        void givenRecordsExistAndOpenRecordsOnly_whenGettingAllSeriesWatchlogIDs_thenReturnsListContainingAllOpenSeriesWatchlogIDs() {
            try {
                WatchlogDBManager.startWatchlogDB(true);

                WatchlogDBManager.addCompleteSeriesWatchlog("435", "2021-02-01", "2021-02-02");
                WatchlogDBManager.setStartDateInSeriesWatchlog("180", "2021-03-01");
                List<String> allSeriesWatchlogIDs = WatchlogDBManager.getAllSeriesWatchlogIDs(true);

                WatchlogDBManager.closeWatchlogDB();

                assertEquals(1, allSeriesWatchlogIDs.size());
                assertTrue(allSeriesWatchlogIDs.contains("2"));

            } catch (Exception e) {
                fail();
            }
        }

        // whenCheckingIsTrackedSeries

        @Test
        void givenTrackedSeriesID_whenCheckingIsTrackedSeries_thenReturnsTrue() {
            try {
                WatchlogDBManager.startWatchlogDB(true);
                WatchlogDBManager.addSeriesToTracked("435"); // TVMazeID for "Smallville"
                assertTrue(WatchlogDBManager.isTrackedSeries("435"));
                WatchlogDBManager.closeWatchlogDB();
            } catch (Exception e) {
                fail();
            }
        }

        @Test
        void givenNotTrackedSeriesID_whenCheckingIsTrackedSeries_thenReturnsFalse() {
            try {
                WatchlogDBManager.startWatchlogDB(true);
                assertFalse(WatchlogDBManager.isTrackedSeries("435"));
                WatchlogDBManager.closeWatchlogDB();
            } catch (Exception e) {
                fail();
            }
        }

        @Test
        void givenInvalidSeriesID_whenCheckingIsTrackedSeries_thenThrowsSQLException() {
            try {
                WatchlogDBManager.startWatchlogDB(true);
                assertThrows(SQLException.class, () -> {
                    assertFalse(WatchlogDBManager.isTrackedSeries("notAValidID"));
                });
                WatchlogDBManager.closeWatchlogDB();
            } catch (Exception e) {
                fail();
            }
        }

        @Test
        void givenEmptySeriesIDString_whenCheckingIsTrackedSeries_thenThrowsSQLException() {
            try {
                WatchlogDBManager.startWatchlogDB(true);
                assertThrows(SQLException.class, () -> {
                    assertFalse(WatchlogDBManager.isTrackedSeries(""));
                });
                WatchlogDBManager.closeWatchlogDB();
            } catch (Exception e) {
                fail();
            }
        }

        // whenCheckingSeriesWatchlogIDExists

        @Test
        void givenSeriesWatchlogIDExists_whenCheckingSeriesWatchlogIDExists_thenReturnsTrue() {
            try {
                WatchlogDBManager.startWatchlogDB(true);

                WatchlogDBManager.setStartDateInSeriesWatchlog("180", "2021-03-01");
                assertTrue(WatchlogDBManager.checkSeriesWatchlogIDExists("1"));

                WatchlogDBManager.closeWatchlogDB();
            } catch (Exception e) {
                fail();
            }
        }

        @Test
        void givenSeriesWatchlogIDNotExists_whenCheckingSeriesWatchlogIDExists_thenReturnsFalse() {
            try {
                WatchlogDBManager.startWatchlogDB(true);

                assertFalse(WatchlogDBManager.checkSeriesWatchlogIDExists("1"));

                WatchlogDBManager.closeWatchlogDB();
            } catch (Exception e) {
                fail();
            }
        }

        @Test
        void givenInvalidSeriesWatchlogID_whenCheckingSeriesWatchlogIDExists_thenReturnsFalse() {
            try {
                WatchlogDBManager.startWatchlogDB(true);

                assertFalse(WatchlogDBManager.checkSeriesWatchlogIDExists("notAValidSeriedID"));

                WatchlogDBManager.closeWatchlogDB();
            } catch (Exception e) {
                fail();
            }
        }

        @Test
        void givenEmptySeriesWatchlogIDString_whenCheckingSeriesWatchlogIDExists_thenReturnsFalse() {
            try {
                WatchlogDBManager.startWatchlogDB(true);

                assertFalse(WatchlogDBManager.checkSeriesWatchlogIDExists(""));

                WatchlogDBManager.closeWatchlogDB();
            } catch (Exception e) {
                fail();
            }
        }

        // whenCheckingExistsOpenSeriesWatchlog

        @Test
        void givenOpenWatchlogExists_whenCheckingExistsOpenSeriesWatchlog_thenReturnsTrue() {
            try {
                WatchlogDBManager.startWatchlogDB(true);

                WatchlogDBManager.setStartDateInSeriesWatchlog("180", "2021-03-01");
                assertTrue(WatchlogDBManager.existsOpenSeriesWatchlog("180"));

                WatchlogDBManager.closeWatchlogDB();
            } catch (Exception e) {
                fail();
            }
        }

        @Test
        void givenOpenWatchlogNotExists_whenCheckingExistsOpenSeriesWatchlog_thenReturnsFalse() {
            try {
                WatchlogDBManager.startWatchlogDB(true);

                WatchlogDBManager.addCompleteSeriesWatchlog("180", "2021-03-01", "");
                assertFalse(WatchlogDBManager.existsOpenSeriesWatchlog("180"));

                WatchlogDBManager.closeWatchlogDB();
            } catch (Exception e) {
                fail();
            }
        }

        @Test
        void givenInvalidSeriesID_whenCheckingExistsOpenSeriesWatchlog_thenThrowsSQLException() {
            try {
                WatchlogDBManager.startWatchlogDB(true);

                assertThrows(SQLException.class, () -> {
                    WatchlogDBManager.existsOpenSeriesWatchlog("notAValidSeriedID");
                });

                WatchlogDBManager.closeWatchlogDB();
            } catch (Exception e) {
                fail();
            }
        }

        @Test
        void givenEmptySeriesIDString_whenCheckingExistsOpenSeriesWatchlog_thenThrowsSQLException() {
            try {
                WatchlogDBManager.startWatchlogDB(true);

                assertThrows(SQLException.class, () -> {
                    WatchlogDBManager.existsOpenSeriesWatchlog("");
                });

                WatchlogDBManager.closeWatchlogDB();
            } catch (Exception e) {
                fail();
            }
        }

    }

}
