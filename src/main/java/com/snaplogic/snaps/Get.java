package com.snaplogic.snaps;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.snaplogic.api.ConfigurationException;
import com.snaplogic.api.ExecutionException;
import com.snaplogic.common.properties.builders.PropertyBuilder;
import com.snaplogic.snap.api.*;
import com.snaplogic.snap.api.capabilities.*;
import com.snaplogic.snap.api.rest.RestHttpClient;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

@SuppressWarnings("ALL")
@General(title = "STF Get", purpose = "To fetch data from given URL")
@Inputs(max = 1, accepts = {ViewType.DOCUMENT})
@Outputs(min = 1, max = 1, offers = {ViewType.DOCUMENT})
@Version(snap = 1)
@Category(snap = SnapCategory.READ)
public class Get extends SimpleSnap {

    private static final Logger log = LoggerFactory.getLogger(Get.class);
    private static final String GET_URL_FIELD_PROP = "getURL";
    private static final String GET_URL_FIELD_LABEL = "URL";
    private static final String GET_URL_FIELD_DESC = "URL used to place GET Request";
    private static final String ERR_FETCHING_GROUP_DATA_MSG = "Error while fetching Group Data results.";
    private String url;
    private final RestHttpClient restHttpClient = new RestHttpClient();
    private static final int SOCKET_TIMEOUT_SEC = 900;
    private static final int CONN_TIMEOUT_SEC = 30;
    protected final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String ERR_UNSUPPORTED_HTTP_METHOD_MSG = "Unsupported HTTP method: %s";
    @Inject
    protected OutputViews outputViews;
    @Inject
    protected DocumentUtility documentUtility;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public void defineProperties(PropertyBuilder propertyBuilder) {
        propertyBuilder.describe(GET_URL_FIELD_PROP, GET_URL_FIELD_LABEL, GET_URL_FIELD_DESC)
                .required()
                .add();
    }

    @Override
    public void configure(PropertyValues propertyValues) throws ConfigurationException {
        setUrl(propertyValues.get(GET_URL_FIELD_PROP));
    }

    @Override
    protected void process(Document document, String s) {
        log.debug("GET URL provided by User: ", getUrl());
        Map groupData = getGroupData(getUrl());
        outputViews.write(documentUtility.newDocument(groupData));
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
                log.error("Unsupported HTTP method {} encountered", httpMethod);
                throw new ExecutionException(String.format(ERR_UNSUPPORTED_HTTP_METHOD_MSG,
                        httpMethod))
                        .withResolutionAsDefect();
        }
    }

    private Map getGroupData(String url) {
        log.info("Fetching Group Data using GET URL provided by User");
        HttpUriRequest httpRequest = createHttpRequest(HttpGet.METHOD_NAME, url);
        HttpResponse httpResponse;
        Map map = Maps.newHashMap();
        try {
            httpResponse = restHttpClient.executeRequest(httpRequest, false,
                    SOCKET_TIMEOUT_SEC, CONN_TIMEOUT_SEC, true);
            StatusLine statusLine = httpResponse.getStatusLine();
            HttpEntity entity = httpResponse.getEntity();
            if (statusLine != null &&
                    statusLine.getStatusCode() >= HttpStatus.SC_OK &&
                    statusLine.getStatusCode() <= HttpStatus.SC_ACCEPTED &&
                    entity != null) {
                map = OBJECT_MAPPER.readValue(entity.getContent(), Map.class);
            }
        } catch (IOException ioException) {
            log.error("Error while fetching Group Data", ioException);
            throw new ExecutionException(ioException, ERR_FETCHING_GROUP_DATA_MSG)
                    .formatWith(ioException.getMessage());
        }
        return map;
    }
}