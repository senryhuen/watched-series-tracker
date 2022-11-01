package com.apiconnector;

import java.io.IOException;
import java.net.ConnectException;
import java.net.http.HttpResponse;

import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;

import com.apimanager.TVMazeAPI;

public class APIConnectorTest {

    /**
     * Tests related to non-static method
     * {@link APIConnector#getResponse(String, boolean)}
     */
    @Nested
    class NonStaticGetResponseTests {

        APIConnector testAPIConnector = Mockito.spy(APIConnector.class);

        @BeforeEach
        void setUp() {
            testAPIConnector.setBaseUrl("https://api.tvmaze.com");
        }

        @Test
        void givenValidBaseURLAndEndpoint_whenGettingValueFromResponseBody_thenCorrectValue()
                throws IOException, InterruptedException {
            HttpResponse<String> response = testAPIConnector.getResponse("/shows/435", true);
            JSONObject responseJSON = new JSONObject(response.body());
            assertEquals("435", responseJSON.get("id").toString());
        }

        @Test
        void givenValidBaseURLAndEndpointAndFollowingRedirects_whenGettingResponseWithNoRedirect_thenResponseCode200()
                throws IOException, InterruptedException {
            HttpResponse<String> response = testAPIConnector.getResponse("/shows/435", true);
            assertEquals(200, response.statusCode());
        }

        @Test
        void givenValidBaseURLAndEndpointAndNotFollowingRedirects_whenGettingResponseWithNoRedirect_thenResponseCode200()
                throws IOException, InterruptedException {
            HttpResponse<String> response = testAPIConnector.getResponse("/shows/435", false);
            assertEquals(200, response.statusCode());
        }

        @Test
        void givenValidBaseURLAndEndpointAndFollowingRedirects_whenGettingResponseContainingRedirect_thenResponseCode200()
                throws IOException, InterruptedException {
            HttpResponse<String> response = testAPIConnector.getResponse("/lookup/shows?imdb=tt0279600", true);
            assertEquals(200, response.statusCode());
        }

        @Test
        void givenValidBaseURLAndEndpointAndNotFollowingRedirects_whenGettingResponseContainingRedirect_thenResponseCode301()
                throws IOException, InterruptedException {
            HttpResponse<String> response = testAPIConnector.getResponse("/lookup/shows?imdb=tt0279600", false);
            assertEquals(301, response.statusCode());
        }

        @Test
        void givenInvalidEndpoint_whenGettingResponse_thenResponseCode404()
                throws IOException, InterruptedException {
            HttpResponse<String> response = testAPIConnector.getResponse("/invalidendpoint", false);
            assertEquals(404, response.statusCode());
        }

        @Test
        void givenInvalidBaseURL_whenGettingResponse_thenThrowsConnectException() {
            testAPIConnector.setBaseUrl("https://invalidbaseurl.tvmaze.com");
            assertThrows(ConnectException.class, () -> {
                testAPIConnector.getResponse("/shows/435", false);
            });
        }

    }

    /**
     * Tests related to static method
     * {@link APIConnector#getResponse(String, String, boolean)}
     */
    @Nested
    class StaticGetResponseTests {

        @Test
        void givenValidBaseURLAndEndpoint_whenGettingValueFromResponseBody_thenCorrectValue()
                throws IOException, InterruptedException {
            HttpResponse<String> response = TVMazeAPI.getResponse("https://api.tvmaze.com", "/shows/435", false);
            JSONObject responseJSON = new JSONObject(response.body());
            assertEquals("435", responseJSON.get("id").toString());
        }

        @Test
        void givenValidBaseURLAndEndpointAndFollowingRedirects_whenGettingResponseWithNoRedirect_thenResponseCode200()
                throws IOException, InterruptedException {
            HttpResponse<String> response = APIConnector.getResponse("https://api.tvmaze.com", "/shows/435", true);
            assertEquals(200, response.statusCode());
        }

        @Test
        void givenValidBaseURLAndEndpointAndNotFollowingRedirects_whenGettingResponseWithNoRedirect_thenResponseCode200()
                throws IOException, InterruptedException {
            HttpResponse<String> response = APIConnector.getResponse("https://api.tvmaze.com", "/shows/435", false);
            assertEquals(200, response.statusCode());
        }

        @Test
        void givenValidBaseURLAndEndpointAndFollowingRedirects_whenGettingResponseContainingRedirect_thenResponseCode200()
                throws IOException, InterruptedException {
            HttpResponse<String> response = APIConnector.getResponse("https://api.tvmaze.com",
                    "/lookup/shows?imdb=tt0279600", true);
            assertEquals(200, response.statusCode());
        }

        @Test
        void givenValidBaseURLAndEndpointAndNotFollowingRedirects_whenGettingResponseContainingRedirect_thenResponseCode301()
                throws IOException, InterruptedException {
            HttpResponse<String> response = APIConnector.getResponse("https://api.tvmaze.com",
                    "/lookup/shows?imdb=tt0279600", false);
            assertEquals(301, response.statusCode());
        }

        @Test
        void givenValidBaseURLAndInvalidEndpoint_whenGettingResponse_thenResponseCode404()
                throws IOException, InterruptedException {
            HttpResponse<String> response = APIConnector.getResponse("https://api.tvmaze.com",
                    "/invalidendpoint", false);
            assertEquals(404, response.statusCode());
        }

        @Test
        void givenInvalidBaseURL_whenGettingResponse_thenThrowsConnectException() {
            assertThrows(ConnectException.class, () -> {
                TVMazeAPI.getResponse("https://invalidbaseurl.tvmaze.com", "/shows/435", false);
            });
        }

    }

    @Test
    void givenResponseWithRedirect_whenGettingRedirectURL_thenReturnsCorrectURL()
            throws IOException, InterruptedException {
        HttpResponse<String> response = APIConnector.getResponse("https://api.tvmaze.com",
                "/lookup/shows?imdb=tt0279600", false);
        assertEquals("https://api.tvmaze.com/shows/435", APIConnector.getRedirectUrl(response));
    }

    @Test
    void givenResponseWithoutRedirect_whenGettingRedirectURL_thenThrowsIllegalArgumentException()
            throws IOException, InterruptedException {
        HttpResponse<String> response = APIConnector.getResponse("https://api.tvmaze.com", "/shows/435", false);

        assertThrows(IllegalArgumentException.class, () -> {
            APIConnector.getRedirectUrl(response);
        });
    }

}
