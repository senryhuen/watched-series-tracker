package com.watchlogcli;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Scanner;

import com.apimanager.TVMazeAPI;
import com.sqlmanager.SQLManager;

public class Main {

    /**
     * Main method. Runs the command line interface.
     */
    public static void main(String[] args) {
        System.out.println("Starting...");

        try {
            WatchlogDBManager.startWatchlogDB();
            UserWatchlogCLI.runCLI();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            WatchlogDBManager.closeWatchlogDB();
        }

    }

    /**
     * A <code>private</code> <code>static</code> class for creating the user
     * interface (CLI), intended to be used in <code>main</code> class/method.
     * 
     * <p>
     * The user interface allows usage and interaction with the main database (view,
     * update, add or remove an entry which shows when a series was watched).
     * </p>
     */
    private static class UserWatchlogCLI {

        private static final String exceptionMessageYNInputFailed = "Input (of either [y,n,b]) failed";

        private static Scanner scanner = new Scanner(System.in);
        private static int currentMenu = 0;

        /**
         * Runs the command line interface for interacting with the database/watchlogs.
         */
        public static void runCLI() {
            int choice, status = 0;

            while (status == 0) {
                showMenu(currentMenu);

                try {
                    choice = Integer.parseInt(getInput());
                    status = doAction(currentMenu, choice);
                } catch (NumberFormatException e) {
                    System.out.println("Invalid input (not an integer), please try again.\n");
                } catch (Exception e) {
                    scanner.close();
                    throw new RuntimeException(e);
                }
            }

            scanner.close();
        }

        /**
         * Gets user input from command line interface.
         * 
         * @return User input as a <code>String</code>
         */
        private static String getInput() {
            return scanner.nextLine();
        }

        /**
         * Loops prompt and getting input until the input is valid ("y", "n" or "b").
         * Intended for [yes/no] prompts.
         * 
         * @param prompt Message to be printed to prompt input
         * @return Either "y", "n", or "b" depending on the input
         */
        private static String getValidYNInput(String prompt) {
            String input;

            while (true) {
                System.out.print(prompt);
                input = getInput().toLowerCase();

                switch (input) {
                    case "y":
                        return "y";
                    case "n":
                        return "n";
                    case "b":
                        return "b";
                    default:
                        System.out.println("invalid input, please try again");
                }
            }
        }

        /**
         * Outputs (prints) a menu selected by <code>menuNum</code>.
         * 
         * @param menuNum ID of (sub)menu
         */
        private static void showMenu(int menuNum) {
            String menu;

            switch (menuNum) {
                case 0:
                    menu = "\nOptions:\n 1) View Watchlog\n 2) Update entry in Watchlog\n 3) Add entry to Watchlog\n 4) Remove entry in Watchlog\n 5) Exit\n\nEnter a number [1-5] to select one of the options above: ";
                    break;
                case 1:
                    menu = "\nView Watchlog Options:\n 1) View list of Series IDs\n 2) View full watchlog\n 3) Go back\n\nEnter a number [1-3] to select one of the options above: ";
                    break;
                default:
                    throw new RuntimeException("Selected menu is invalid");
            }

            System.out.print(menu);
        }

        /**
         * Performs actions corresponding to a selected option in a menu.
         * 
         * @param menuNum   ID of menu which <code>optionNum</code> is selected from
         * @param optionNum ID of option in a menu
         * @return 1 to indicate exit, 0 to continue (back to menu)
         * @throws SQLException
         * @throws IOException
         * @throws InterruptedException
         */
        private static int doAction(int menuNum, int optionNum) throws SQLException, IOException, InterruptedException {
            switch (menuNum) {
                case 0: // main menu
                    switch (optionNum) {
                        case 1: // view watchlog
                            currentMenu = 1;
                            break;
                        case 2:
                            doActionUpdateEntry();
                            break;
                        case 3:
                            doActionAddEntry();
                            break;
                        case 4:
                            doActionRemoveEntry();
                            break;
                        case 5: // exit
                            return 1;
                        default: // invalid option
                            System.out.printf("Option '%d' does not exist, please try again.\n", optionNum);
                    }

                    break;
                case 1: // sub menu for "view watchlog"
                    switch (optionNum) {
                        case 1: // view list/table of series ids
                            System.out.println();
                            WatchlogDBManager.printSimpleTable(false);
                            break;
                        case 2: // view full watchlog
                            System.out.println();
                            WatchlogDBManager.printFullWatchlogTable(false);
                            break;
                        case 3: // go back
                            currentMenu = 0;
                            break;
                        default: // invalid option
                            System.out.printf("Option '%d' does not exist, please try again.\n", optionNum);
                    }

                    break;
                default:
                    throw new RuntimeException("Selected menu is invalid");
            }

            return 0;
        }

        /**
         * Command line interface to get input of a <code>series_watchlog_id</code>,
         * which can be filtered by <code>openRecordsOnly</code>.
         * 
         * @param openRecordsOnly If <code>true</code>, IDs of non-open (finished)
         *                        series watchlog entries are considered invalid
         * @return Valid <code>series_watchlog_id</code> as a <code>String</code> or "b"
         *         to indicate "go back"
         * @throws SQLException
         */
        private static String cliGetSeriesWatchlogID(boolean openRecordsOnly) throws SQLException {
            List<String> allValidIDs = WatchlogDBManager.getAllSeriesWatchlogIDs(openRecordsOnly);

            String seriesWatchlogID;
            while (true) {
                System.out.print(
                        "\nEnter the series_watchlog_id of the record you would like to select [or enter b to go back]: ");
                seriesWatchlogID = getInput();

                if (seriesWatchlogID.toLowerCase().equals("b")) { // go back
                    break;
                } else if (allValidIDs.contains(seriesWatchlogID)) { // valid ID
                    System.out.println();
                    WatchlogDBManager.printRowOfSeriesWatchlog(Integer.parseInt(seriesWatchlogID), true);
                    break;
                } else { // invalid input
                    System.out.println("invalid ID, please try again");
                }
            }

            return seriesWatchlogID;
        }

        /**
         * Command line interface to get a valid date in the format "YYYY-MM-DD" (either
         * from
         * using the current date, or manual input).
         * 
         * <p>
         * eg. "2021-02-01" for 1st February 2021
         * </p>
         * 
         * @return Valid date in the format "YYYY-MM-DD", or "b" to indicate "go back"
         */
        private static String cliGetDate() {
            String prompt = "\nWould you like to use the current date? [y/n] [or enter b to go back]: ";
            String useCurrentDate, date = "";

            while (true) {
                useCurrentDate = getValidYNInput(prompt);
                switch (useCurrentDate) {
                    case "y": // use current date
                        date = SQLManager.getCurrentDate();
                        break;
                    case "n": // get date from input
                        date = cliGetDateInput();

                        if (date.equals("b")) {
                            continue;
                        }

                        break;
                    case "b": // go back
                        break;
                    default: // should not happen
                        throw new RuntimeException(exceptionMessageYNInputFailed);
                }

                return date;
            }
        }

        /**
         * Command line interface to get valid manual input of a date in the format
         * "YYYY-MM-DD".
         * 
         * @return Valid date in the format "YYYY-MM-DD", or "b" to indicate "go back"
         */
        private static String cliGetDateInput() {
            String prompt = "\nEnter the day in the format 'YYYY-MM-DD' (eg. '2021-02-01' for 1st February 2021) [or enter b to go back]: ";
            String date;

            while (true) {
                System.out.print(prompt);
                date = getInput();

                if (date.toLowerCase().equals("b")) { // go back
                    break;
                } else if (SQLManager.isISOLocalDateFormat(date)) { // valid date
                    break;
                } else { // invalid input
                    System.out.println("invalid date, please try again");
                }
            }

            return date;
        }

        /**
         * Command line interface to get input of valid <code>TVMazeID</code> of a
         * series to add to watchlog.
         * 
         * @return Valid <code>TVMazeID</code>, or "b" to indicate "go back"
         * @throws SQLException
         * @throws IOException
         * @throws InterruptedException
         */
        private static String addEntryGetSeriesID() throws SQLException, IOException, InterruptedException {
            System.out.println("\nCurrently tracked series:");
            WatchlogDBManager.printSimpleTable(false);

            String prompt = "\nEnter the series_id (TVMaze ID) of the series you would like to add a watchlog for [or enter b to go back]: ";
            String seriesID;

            while (true) {
                System.out.print(prompt);
                seriesID = getInput();

                if (seriesID.toLowerCase().equals("b")) { // go back
                    break;
                } else if (TVMazeAPI.validateTVMazeID(seriesID)) { // valid seriesID
                    break;
                } else { // invalid input
                    System.out.println("invalid ID, please try again");
                }
            }

            return seriesID;
        }

        /**
         * Performs action corresponding to the "update entry" option in
         * {@link #doAction(int, int)}
         * 
         * @throws SQLException
         */
        private static void doActionUpdateEntry() throws SQLException {
            // print open records to show options
            System.out.println();
            WatchlogDBManager.printOpenSeriesWatchlogTable();

            // get series_watchlog_id of entry to update
            String seriesWatchlogID = cliGetSeriesWatchlogID(true);
            if (seriesWatchlogID.equals("b")) {
                return;
            }

            // get date to update finish_date to
            System.out.println("\nUpdating finish_date:");
            String date = cliGetDate();
            if (date.equals("b")) {
                return;
            }

            // update finish_date in DB
            String seriesID = WatchlogDBManager.getSeriesIDFromSeriesWatchlogID(seriesWatchlogID);
            try {
                WatchlogDBManager.setFinishDateInSeriesWatchlog(seriesID, date);
            } catch (Exception e) {
                System.out.printf("Failed to update watchlog (entry ID: %s).\n", seriesWatchlogID);
            }

            System.out.printf("Successfully updated entry.\n");
        }

        /**
         * Performs action corresponding to the "add entry" option in
         * {@link #doAction(int, int)}
         * 
         * @throws SQLException
         * @throws IOException
         * @throws InterruptedException
         */
        private static void doActionAddEntry() throws SQLException, IOException, InterruptedException {
            // get seriesID of entry
            String seriesID = addEntryGetSeriesID();
            if (seriesID.equals("b")) {
                return;
            }

            // get start date of entry
            System.out.println("\nStart Date:");
            String startDate = cliGetDate();
            if (startDate.equals("b")) {
                return;
            }

            // get finish date of entry if applicable
            String input = "b", finishDate = "";
            boolean completeRecord = false; // complete record means record will be marked as finished
            while (true) {
                input = getValidYNInput("\nWould you like to include a finish date? [y/n] [or enter b to go back]: ");

                switch (input) {
                    case "y": // add finish date
                        System.out.println("Finish Date:");
                        finishDate = cliGetDate();
                        if (finishDate.equals("b")) {
                            continue;
                        }
                        break;
                    case "n": // skip finish date
                        input = getValidYNInput("\nIs this watchlog entry complete? [y/n] [or enter b to go back]: ");
                        switch (input) {
                            case "y":
                                completeRecord = true;
                                break;
                            case "n":
                                completeRecord = false;
                                break;
                            case "b":
                                continue;
                            default: // should not happen
                                throw new RuntimeException(exceptionMessageYNInputFailed);
                        }

                        break;
                    case "b": // go back
                        finishDate = "b";
                        break;
                    default: // should not happen
                        throw new RuntimeException(exceptionMessageYNInputFailed);
                }

                break;
            }

            if (input.equals("b") || finishDate.equals("b")) {
                return;
            }

            // add record in DB
            if (completeRecord) {
                try {
                    WatchlogDBManager.addCompleteSeriesWatchlog(seriesID, startDate, finishDate);
                } catch (DBRollbackOccurredException e) {
                    System.out.println("***Action failed, no changes were made.***");
                }
            } else {
                WatchlogDBManager.setStartDateInSeriesWatchlog(seriesID, startDate);
            }

            System.out.printf("Successfully updated entry.\n");
        }

        /**
         * Performs action corresponding to the "remove entry" option in
         * {@link #doAction(int, int)}
         * 
         * @throws SQLException
         */
        private static void doActionRemoveEntry() throws SQLException {
            // print all records to show options
            System.out.println();
            WatchlogDBManager.printFullWatchlogTable(false);

            // get series_watchlog_id of entry
            String seriesWatchlogID = cliGetSeriesWatchlogID(false);
            if (seriesWatchlogID.equals("b")) {
                return;
            }

            // confirmation of deletion
            String cont = getValidYNInput("\nWould you like to continue? [y/n] [or enter b to go back]: ");
            switch (cont) {
                case "y": // delete entry
                    WatchlogDBManager.removeFromSeriesWatchlog(seriesWatchlogID);
                    System.out.printf("(%s) deleted from series_watchlog successfully\n", seriesWatchlogID);
                    break;
                case "n": // cancel deletion
                    System.out.println("Aborted deletion");
                    break;
                case "b": // go back
                    break;
                default: // should not happen
                    throw new RuntimeException(exceptionMessageYNInputFailed);
            }
        }

    }

}
