package com.apimanager;

import java.io.IOException;
import java.net.http.HttpResponse;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.apiconnector.APIConnector;

/**
 * A class for interacting with TVMaze's API.
 * 
 * <p>
 * The TVMaze API provides information on tv series and episodes. This class
 * contains methods which loads the data in JSON format which is then queried.
 * </p>
 * 
 * @author senryhuen
 */
public class TVMazeAPI extends APIConnector {
    private String seriesID = "";
    private JSONObject seriesInfo;
    private JSONArray seriesEpisodesInfo;

    /**
     * Initialises <code>TVMazeAPI</code> instance and loads information for a
     * specified series.
     * 
     * @param seriesID TVMaze ID or IMDb ID of series to load (depending on
     *                 <code>isIMDbID</code>)
     * @param isIMDbID <code>seriesID</code> is an IMDb ID if <code>true</code>,
     *                 TVMaze ID if <code>false</code>
     * @throws IOException
     * @throws InterruptedException
     */
    public TVMazeAPI(String seriesID, boolean isIMDbID) throws IOException, InterruptedException {
        super.setBaseUrl("https://api.tvmaze.com");

        if (isIMDbID) {
            setCurrentSeriesFromIMDbID(seriesID);
        } else {
            setCurrentSeries(seriesID);
        }
    }

    /**
     * Indentical to {@link #TVMazeAPI(String, boolean)} constructor, but
     * <code>seriesID</code> is asssumed to be a TVMaze ID.
     * 
     * @param seriesID TVMaze ID (different to IMDb ID) of series to load
     * @throws IOException
     * @throws InterruptedException
     */
    public TVMazeAPI(String seriesID) throws IOException, InterruptedException {
        super.setBaseUrl("https://api.tvmaze.com");

        setCurrentSeries(seriesID);
    }

    /**
     * Static method intended for use outside of object as a utility. Not most
     * efficient for use within object.
     * 
     * @param IMDbID IMDb ID of a series
     * @return TVMaze ID corresponding to series identified by <code>IMDbID</code>
     * @throws IOException
     * @throws InterruptedException
     */
    public static String getTVMazeIDFromIMDbID(String IMDbID) throws IOException, InterruptedException {
        String endpoint = "/lookup/shows?imdb=" + IMDbID;

        HttpResponse<String> response = APIConnector.getResponse("https://api.tvmaze.com", endpoint, true);

        if (response.statusCode() != 200) {
            throw new IllegalArgumentException("IMDbID: Not a valid ID");
        }

        JSONObject responseJSON = new JSONObject(response.body());
        return responseJSON.get("id").toString();
    }

    /**
     * Checks <code>TVMazeID</code> exists.
     * 
     * <p>
     * Static method intended for use outside of object as a utility. Not efficient
     * for use within object.
     * </p>
     * 
     * @param TVMazeID <code>String</code> to check is a valid TVMaze ID
     * @return <code>true</code> if <code>TVMazeID</code> is a valid ID,
     *         <code>false</code> otherwise
     * @throws IOException
     * @throws InterruptedException
     */
    public static boolean validateTVMazeID(String TVMazeID) throws IOException, InterruptedException {
        String endpoint = "/shows/" + TVMazeID;
        HttpResponse<String> response = APIConnector.getResponse("https://api.tvmaze.com", endpoint, false);

        if (response.statusCode() == 200) {
            return true;
        } else if (response.statusCode() == 301) {
            throw new RuntimeException("HttpResponseCode: 301");
        }

        return false;
    }

    /**
     * @return TVMaze ID of series that is currently loaded (series whos information
     *         is stored in current instance of <code>TVMazeAPI</code>)
     */
    public String getCurrentSeriesID() {
        return this.seriesID;
    }

    /**
     * @return IMDb ID of series that is currently loaded (series whos information
     *         is stored in current instance of <code>TVMazeAPI</code>)
     */
    public String getCurrentSeriesIMDbID() throws JSONException {
        return getSeriesInfoJSONObjByTag("externals").get("imdb").toString();
    }

    /**
     * Sets current series by loading information of new series as specified by
     * <code>seriesID</code>.
     * 
     * @param seriesID TVMaze ID (different to IMDb ID) of series to load
     * @throws IOException
     * @throws InterruptedException
     */
    public void setCurrentSeries(String seriesID) throws IOException, InterruptedException {
        this.seriesID = seriesID;
        loadSeriesInfo();
        loadSeriesEpisodesInfo();
    }

    /**
     * Same as {@link #setCurrentSeries(String)}, but <code>seriesID</code> is an
     * IMDbID rather than TVMazeID.
     * 
     * @param seriesID IMDb ID of series to load
     * @throws IOException
     * @throws InterruptedException
     */
    public void setCurrentSeriesFromIMDbID(String seriesID) throws IOException, InterruptedException {
        loadSeriesInfoFromIMDbID(seriesID);
        this.seriesID = getSeriesInfoByTag("id");
        loadSeriesEpisodesInfo();
    }

    /**
     * @return Total number of episodes in loaded series across all seasons
     */
    public int getNumEpisodes() {
        return this.seriesEpisodesInfo.length();
    }

    /**
     * @return Name/title of loaded series
     */
    public String getSeriesName() {
        return getSeriesInfoByTag("name");
    }

    /**
     * @return Status of loaded series (whether the series is "Running", "Ended", or
     *         "To Be Determined")
     */
    public String getSeriesStatus() {
        return getSeriesInfoByTag("status");
    }

    /**
     * @return Date - in string format "YYYY-MM-DD" - of when loaded series
     *         premiered
     */
    public String getSeriesPremiereDate() {
        return getSeriesInfoByTag("premiered");
    }

    /**
     * @return Date - in string format "YYYY-MM-DD" - of when loaded series ended,
     *         <code>null</code> if series has not ended yet
     */
    public String getSeriesEndedDate() {
        return getSeriesInfoByTag("ended");
    }

    /**
     * @param overallEpisodeNum Overall episode number, which is the episode number
     *                          when episodes are not divided into seasons -
     *                          NOT episode ID
     * @return Name of episode at <code>episodeIndex</code>
     */
    public String getEpisodeName(int overallEpisodeNum) {
        String episodeIndex = String.valueOf(overallEpisodeNum - 1);
        return getEpisodeInfoByTag(episodeIndex, "name");
    }

    /**
     * @param overallEpisodeNum Overall episode number, which is the episode number
     *                          when episodes are not divided into seasons -
     *                          NOT episode ID
     * @return Season number that the episode at <code>episodeIndex</code> is in
     */
    public String getEpisodeSeasonNum(int overallEpisodeNum) {
        String episodeIndex = String.valueOf(overallEpisodeNum - 1);
        return getEpisodeInfoByTag(episodeIndex, "season");
    }

    /**
     * @param overallEpisodeNum Overall episode number, which is the episode number
     *                          when episodes are not divided into seasons -
     *                          NOT episode ID
     * @return Episode number (within season) of the episode at
     *         <code>episodeIndex</code>, may be <code>null</code> for special
     *         episodes
     */
    public String getEpisodeNum(int overallEpisodeNum) {
        String episodeIndex = String.valueOf(overallEpisodeNum - 1);
        return getEpisodeInfoByTag(episodeIndex, "number");
    }

    /**
     * @param overallEpisodeNum Overall episode number, which is the episode number
     *                          when episodes are not divided into seasons -
     *                          NOT episode ID
     * @return "Episode ID" of episode at <code>episodeIndex</code>
     */
    public String getEpisodeID(int overallEpisodeNum) {
        String episodeIndex = String.valueOf(overallEpisodeNum - 1);
        return getEpisodeInfoByTag(episodeIndex, "id");
    }

    /**
     * @param overallEpisodeNum Overall episode number, which is the episode number
     *                          when episodes are not divided into seasons -
     *                          NOT episode ID
     * @return Date - in string format "YYYY-MM-DD" - that episode at
     *         <code>episodeIndex</code> aired
     */
    public String getEpisodeAirDate(int overallEpisodeNum) {
        String episodeIndex = String.valueOf(overallEpisodeNum - 1);
        return getEpisodeInfoByTag(episodeIndex, "airdate");
    }

    /**
     * @param overallEpisodeNum Overall episode number, which is the episode number
     *                          when episodes are not divided into seasons -
     *                          NOT episode ID
     * @return Approximate length of episode at <code>episodeIndex</code> in minutes
     */
    public String getEpisodeRuntime(int overallEpisodeNum) {
        String episodeIndex = String.valueOf(overallEpisodeNum - 1);
        return getEpisodeInfoByTag(episodeIndex, "runtime");
    }

    /**
     * Loads/gets info of selected <code>seriesID</code> from TVMaze API as a
     * <code>JSONObject</code>.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    private void loadSeriesInfo() throws IOException, InterruptedException {
        String endpoint = "/shows/" + this.seriesID;
        HttpResponse<String> response = super.getResponse(endpoint, false);

        if (response.statusCode() == 301) {
            throw new RuntimeException("HttpResponseCode: 301");
        } else if (response.statusCode() != 200) {
            throw new IllegalArgumentException("TVMazeID: Not a valid ID");
        }

        this.seriesInfo = new JSONObject(response.body());
    }

    /**
     * Loads/gets info of all episodes (including specials) in selected
     * <code>seriesID</code> from TVMaze API as a JSON Array.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    private void loadSeriesEpisodesInfo() throws IOException, InterruptedException {
        String endpoint = "/shows/" + this.seriesID + "/episodes?specials=1";
        HttpResponse<String> response = super.getResponse(endpoint, false);

        if (response.statusCode() == 301) {
            throw new RuntimeException("HttpResponseCode: 301");
        } else if (response.statusCode() != 200) {
            throw new RuntimeException(String.format("Episode info not found for seriesID '%s'", this.seriesID));
        }

        this.seriesEpisodesInfo = new JSONArray(response.body());
    }

    /**
     * Same as {@link #loadSeriesInfo()}, but identifies series with IMDbID instead
     * of TVMazeID.
     * 
     * @param IMDbID IMDb ID of series to load info for (from TVMaze's API)
     * @throws IOException
     * @throws InterruptedException
     */
    private void loadSeriesInfoFromIMDbID(String IMDbID) throws IOException, InterruptedException {
        String endpoint = "/lookup/shows?imdb=" + IMDbID;
        HttpResponse<String> response = super.getResponse(endpoint, true);

        if (response.statusCode() != 200) {
            throw new IllegalArgumentException("IMDbID: Not a valid ID");
        }

        this.seriesInfo = new JSONObject(response.body());
    }

    /**
     * Query series info loaded into JSON Object which limits API requests needed.
     * 
     * @param tagKey Name of attribute in JSON Object
     * @return Value at <code>tagKey</code> in JSON Object
     */
    private String getSeriesInfoByTag(String tagKey) {
        return this.seriesInfo.get(tagKey).toString();
    }

    /**
     * Query episode info loaded into JSON Array which limits API requests needed.
     * 
     * @param episodeIndex Index of episode in JSON Array of episodes (JSON Objects)
     *                     - NOT episode ID
     * @param tagKey       Name of attribute in JSON Object
     * @return Value at <code>tagKey</code> in JSON Object of
     *         <code>episodeIndex</code>
     */
    private String getEpisodeInfoByTag(String episodeIndex, String tagKey) {
        return this.seriesEpisodesInfo.getJSONObject(Integer.parseInt(episodeIndex)).get(tagKey).toString();
    }

    /**
     * Same as {@link #getSeriesInfoByTag(String)}, but returns
     * <code>JSONObject</code> rather than a <code>String</code>.
     * 
     * @param tagKey Name of attribute in JSON Object being queried
     * @return Value at <code>tagKey</code> in JSON Object being queried
     */
    private JSONObject getSeriesInfoJSONObjByTag(String tagKey) {
        return this.seriesInfo.getJSONObject(tagKey);
    }

    // /**
    //  * Same as {@link #getEpisodeInfoByTag(String, String)}, but returns
    //  * <code>JSONObject</code> rather than a String.
    //  * 
    //  * @param episodeIndex Index of episode in JSON Array of episodes (JSON Objects)
    //  *                     - NOT episode ID
    //  * @param tagKey       Name of attribute in JSON Object being queried
    //  * @return Value at <code>tagKey</code> in JSON Object of
    //  *         <code>episodeIndex</code>
    //  */
    // private JSONObject getEpisodeInfoJSONObjByTag(String episodeIndex, String tagKey) {
    //     if (getCurrentSeriesID().isEmpty()) { // if no series loaded...
    //         return null;
    //     }

    //     JSONObject outObj = this.seriesEpisodesInfo.getJSONObject(Integer.parseInt(episodeIndex)).getJSONObject(tagKey);
    //     return outObj;
    // }

}
