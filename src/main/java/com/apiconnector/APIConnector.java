package com.apiconnector;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * An abstract class that enables subclasses to access/connect to APIs (with a
 * persistant client).
 * 
 * <p>
 * Uses the same <code>HttpClient</code> throughout the lifespan of an instance
 * rather than a new one for every request (<code>HttpRequest</code>), hence
 * persistant.
 * </p>
 * 
 * @author senryhuen
 */
public abstract class APIConnector {

    private final HttpClient client = HttpClient.newHttpClient();
    private String baseUrl;

    /**
     * @param baseUrl URL that allows access to an API
     */
    protected void setBaseUrl(String newBaseUrl) {
        this.baseUrl = newBaseUrl;
    }

    /**
     * Sends a http request and checks the response.
     * 
     * @param endpoint       Path to concatenate to <code>baseURL</code> to form
     *                       http
     *                       request url
     * @param followRedirect If true, if response is a redirect, gets response of
     *                       the redirected URL, otherwise returns first response
     * @return Response from http request
     * @throws IOException
     * @throws InterruptedException
     */
    protected HttpResponse<String> getResponse(String endpoint, boolean followRedirect)
            throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder()
                .uri(URI.create(this.baseUrl + endpoint))
                .build();

        HttpResponse<String> response = this.client.send(request, HttpResponse.BodyHandlers.ofString());

        // Check API response from connect attempt
        int responseCode = response.statusCode();

        // Get response of redirected URL if necessary
        int redirectCount = 0;
        while (responseCode == 301 && followRedirect) {
            redirectCount++;
            if (redirectCount > 3) {
                throw new IOException(String.format("Too many redirects when requesting URL '%s'", baseUrl + endpoint));
            }

            response = APIConnector.getResponse("", getRedirectUrl(response), false);
            responseCode = response.statusCode();
        }

        // 200: OK, 429: rate limit
        if (responseCode != 200 && responseCode != 301 && responseCode != 404) {
            throw new RuntimeException("HttpResponseCode: " + responseCode);
        }

        return response;
    }

    /**
     * Sends a http request and gets the response.
     * 
     * <p>
     * Same as {@link #getResponse(String, boolean)}, but static. Intended for use
     * outside of an instance, not as efficient for use within instance.
     * </p>
     * 
     * @param baseUrl        URL that allows access to an API
     * @param endpoint       Path to concatenate to <code>baseURL</code> to form
     *                       http
     * @param followRedirect If true, if response is a redirect, gets response of
     *                       the redirected URL, otherwise returns first response
     * @return Response from http request
     * @throws IOException
     * @throws InterruptedException
     */
    public static HttpResponse<String> getResponse(String baseUrl, String endpoint, boolean followRedirect)
            throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + endpoint))
                .build();

        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        // Check API response from connect attempt
        int responseCode = response.statusCode();

        // Get response of redirected URL if necessary
        int redirectCount = 0;
        while (responseCode == 301 && followRedirect) {
            redirectCount++;
            if (redirectCount > 3) {
                throw new IOException(String.format("Too many redirects when requesting URL '%s'", baseUrl + endpoint));
            }

            response = getResponse("", getRedirectUrl(response), false);
            responseCode = response.statusCode();
        }

        // 200: OK, 429: rate limit
        if (responseCode != 200 && responseCode != 301 && responseCode != 404) {
            throw new RuntimeException("HttpResponseCode: " + responseCode);
        }

        return response;
    }

    /**
     * Gets the URL to redirect to from a <code>HttpResponse</code>.
     * 
     * @param response <code>HttpResponse</code> with status code of 301
     * @return URL to redirect to, found in the <code>response</code> header under
     *         "location"
     */
    public static String getRedirectUrl(HttpResponse<String> response) {
        if (response.statusCode() != 301) {
            throw new IllegalArgumentException("Response does not have a redirect");
        }

        return response.headers().allValues("location").get(0);
    }

}
