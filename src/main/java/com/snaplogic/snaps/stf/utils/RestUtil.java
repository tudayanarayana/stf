package com.snaplogic.snaps.stf.utils;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.snaplogic.api.ExecutionException;
import com.snaplogic.snap.api.rest.RestHttpClient;

import org.apache.commons.io.IOUtils;
import org.apache.http.*;
import org.apache.http.client.methods.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static com.snaplogic.snaps.stf.Constants.RESPONSE_TYPE_MAP;
import static com.snaplogic.snaps.stf.Constants.RESPONSE_TYPE_STRING;

public class RestUtil {
    private static final int SOCKET_TIMEOUT_SEC = 900;
    private static final int CONN_TIMEOUT_SEC = 30;
    private final RestHttpClient restHttpClient = new RestHttpClient();
    private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(RestUtil.class);
    private static final String ERR_UNSUPPORTED_HTTP_METHOD_MSG = "Unsupported HTTP method: %s";

    public static class RestResponseObject {
        private StatusLine statusLine;
        private Map body;

        RestResponseObject(StatusLine statusLine) {
            this.statusLine = statusLine;
        }

        RestResponseObject(Map body) {
            this.body = body;
        }

        public StatusLine getStatusLine() {
            return statusLine;
        }

        public Map getBody() {
            return body;
        }
    }

    public RestResponseObject invokeHttpCall(String httpMethod, HttpEntity payload, String url, String responseType) throws IOException {
        HttpUriRequest httpRequest = createHttpRequest(httpMethod, url);
        if (payload != null && httpRequest instanceof HttpEntityEnclosingRequestBase) {
            ((HttpEntityEnclosingRequestBase) httpRequest).setEntity(payload);
        }
        HttpResponse httpResponse = restHttpClient.executeRequest(httpRequest, false,
                SOCKET_TIMEOUT_SEC, CONN_TIMEOUT_SEC, true);
        StatusLine statusLine = httpResponse.getStatusLine();
        HttpEntity entity = httpResponse.getEntity();
        if (statusLine != null &&
                statusLine.getStatusCode() >= HttpStatus.SC_OK &&
                statusLine.getStatusCode() <= HttpStatus.SC_ACCEPTED &&
                entity != null) {
            Map map = Maps.newHashMap();
            try (InputStream inputStream = entity.getContent()) {
                switch (responseType) {
                    case RESPONSE_TYPE_STRING:
                        String valueToken = IOUtils.toString(inputStream, StandardCharsets.UTF_8.name());
                        map.put("valueToken", valueToken);
                        break;
                    case RESPONSE_TYPE_MAP:
                        try {
                            map = OBJECT_MAPPER.readValue(inputStream, Map.class);
                        } catch (JsonMappingException e1) {
                            LOGGER.info("Error occurred while trying to read REST response as a MAP", e1);
                        }
                        break;
                }
            }
            return new RestResponseObject(map);
        } else {
            return new RestResponseObject(statusLine);
        }
    }

    private HttpUriRequest createHttpRequest(String httpMethod, String url) {
        switch (httpMethod) {
            case HttpGet.METHOD_NAME:
                return new HttpGet(url);
            case HttpPost.METHOD_NAME:
                return new HttpPost(url);
            case HttpPut.METHOD_NAME:
                return new HttpPut(url);
            case HttpDelete.METHOD_NAME:
                return new HttpDelete(url);
            case HttpPatch.METHOD_NAME:
                return new HttpPatch(url);
            default:
                LOGGER.error("Unsupported HTTP method {} encountered", httpMethod);
                throw new ExecutionException(String.format(ERR_UNSUPPORTED_HTTP_METHOD_MSG,
                        httpMethod))
                        .withResolutionAsDefect();
        }
    }
}