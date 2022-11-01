package com.watchlogcli;

public class CannotPerformActionException extends Exception {
    public CannotPerformActionException(String errorMessage) {
        super(errorMessage);
    }
}
