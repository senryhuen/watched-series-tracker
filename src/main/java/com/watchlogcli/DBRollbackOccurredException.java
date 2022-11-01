package com.watchlogcli;

public class DBRollbackOccurredException extends Exception {
    public DBRollbackOccurredException(String errorMessage) {
        super(errorMessage);
    }

    public DBRollbackOccurredException(String errorMessage, Throwable e) {
        super(errorMessage, e);
    }
}
