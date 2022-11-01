package com.apimanager;

import java.io.IOException;

import org.json.JSONException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

public class TVMazeAPITest {

    static TVMazeAPI[] tvmazeConnectors;
    int overallEpisodeNum = 3;

    @BeforeAll
    static void setUp() throws IOException, InterruptedException {
        // TVMazeID, IMDbID for "Smallville"
        tvmazeConnectors = new TVMazeAPI[] { new TVMazeAPI("435"), new TVMazeAPI("tt0279600", true) };
    }

    @Test
    void givenInvalidTVMazeID_whenConstructingTVMazeAPI_thenThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> {
            new TVMazeAPI("invalidSeriesID");
        });
    }

    @Test
    void givenInvalidIMDbID_whenConstructingTVMazeAPI_thenThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> {
            new TVMazeAPI("invalidSeriesID", true);
        });
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void givenSeriesInfoLoaded_whenGettingCurrentSeriesID_thenCorrectResult(int x) {
        assertEquals("435", tvmazeConnectors[x].getCurrentSeriesID());
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void givenSeriesInfoLoaded_whenGettingCurrentSeriesIMDbID_thenCorrectResult(int x) {
        assertEquals("tt0279600", tvmazeConnectors[x].getCurrentSeriesIMDbID());
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void givenSeriesInfoLoaded_whenGettingNumEpisodes_thenCorrectResult(int x) {
        assertEquals(217, tvmazeConnectors[x].getNumEpisodes());
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void givenSeriesInfoLoaded_whenGettingSeriesName_thenCorrectResult(int x) {
        assertEquals("Smallville", tvmazeConnectors[x].getSeriesName());
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void givenSeriesInfoLoaded_whenGettingSeriesStatus_thenCorrectResult(int x) {
        assertEquals("Ended", tvmazeConnectors[x].getSeriesStatus());
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void givenSeriesInfoLoaded_whenGettingSeriesPremiereDate_thenCorrectResult(int x) {
        assertEquals("2001-10-16", tvmazeConnectors[x].getSeriesPremiereDate());
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void givenSeriesInfoLoadedOfFinishedSeries_whenGettingEndedDate_thenCorrectResult(int x) {
        assertEquals("2011-05-13", tvmazeConnectors[x].getSeriesEndedDate());
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void givenEpisodeInfoLoadedAndValidArg_whenGettingEpisodeName_thenCorrectResult(int x) {
        assertEquals("Hothead", tvmazeConnectors[x].getEpisodeName(overallEpisodeNum));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void givenEpisodeInfoLoadedAndInvalidArg_whenGettingEpisodeName_thenException(int x) {
        // Invalid arg as overallEpisodeNum value of '300' is out of range (larger than
        // number of episodes).
        assertThrows(JSONException.class, () -> tvmazeConnectors[x].getEpisodeName(300));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void givenEpisodeInfoLoadedAndValidArg_whenGettingEpisodeSeasonNum_thenCorrectResult(int x) {
        assertEquals("1", tvmazeConnectors[x].getEpisodeSeasonNum(overallEpisodeNum));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void givenEpisodeInfoLoadedAndValidArg_whenGettingEpisodeNum_thenCorrectResult(int x) {
        assertEquals("3", tvmazeConnectors[x].getEpisodeNum(overallEpisodeNum));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void givenEpisodeInfoLoadedAndValidArg_whenGettingEpisodeID_thenCorrectResult(int x) {
        assertEquals("41131", tvmazeConnectors[x].getEpisodeID(overallEpisodeNum));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void givenEpisodeInfoLoadedAndValidArg_whenGettingEpisodeAirDate_thenCorrectResult(int x) {
        assertEquals("2001-10-30", tvmazeConnectors[x].getEpisodeAirDate(overallEpisodeNum));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void givenEpisodeInfoLoadedAndValidArg_whenGettingRuntime_thenCorrectResult(int x) {
        assertEquals("60", tvmazeConnectors[x].getEpisodeRuntime(overallEpisodeNum));
    }

    @Test
    void givenValidIMDbID_whenGettingTVMazeIDFromIMDbID_thenCorrectTVMazeID() throws IOException, InterruptedException {
        assertEquals("435", TVMazeAPI.getTVMazeIDFromIMDbID("tt0279600"));
    }

    @Test
    void givenInvalidIMDbID_whenGettingTVMazeIDFromIMDbID_thenThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> {
            TVMazeAPI.getTVMazeIDFromIMDbID("invalidIMDbID");
        });
    }

    @Test
    void givenValidTVMazeID_whenValidatingTVMazeID_thenReturnTrue() throws IOException, InterruptedException {
        assertTrue(TVMazeAPI.validateTVMazeID("435"));
    }

    @Test
    void givenInvalidTVMazeID_whenValidatingTVMazeID_thenReturnFalse() throws IOException, InterruptedException {
        assertFalse(TVMazeAPI.validateTVMazeID("invalidTVMazeID"));
    }

}
