package com.snaplogic.snaps.stf;

import com.google.inject.Inject;
import com.snaplogic.api.ConfigurationException;
import com.snaplogic.api.ExecutionException;
import com.snaplogic.common.properties.builders.PropertyBuilder;
import com.snaplogic.snap.api.*;
import com.snaplogic.snap.api.capabilities.*;
import com.snaplogic.snaps.stf.utils.RestUtil;

import org.apache.http.*;
import org.apache.http.client.methods.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.snaplogic.snaps.stf.Constants.*;
import static com.snaplogic.snaps.stf.utils.RestUtil.RestResponseObject;

@Version(snap = 1)
@General(title = "Get", purpose = "To fetch data from given URL")
@Inputs(max = 1, accepts = {ViewType.DOCUMENT})
@Outputs(min = 1, max = 1, offers = {ViewType.DOCUMENT})
@Category(snap = SnapCategory.READ)
public class Get extends SimpleSnap {
    private static final Logger LOGGER = LoggerFactory.getLogger(Get.class);
    private static final String GET_URL_FIELD_PROP = "getURL";
    private static final String GET_URL_FIELD_LABEL = "URL";
    private static final String GET_URL_FIELD_DESC = "URL used to place GET Request";

    private String url;
    @Inject
    private RestUtil restUtil;

    private String getUrl() {
        return url;
    }
    private void setUrl(String url) {
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

    private Header[] generateHeaders(List<Header> specificHeaders) {
        List<Header> headers = new ArrayList<>();
        if (specificHeaders != null && !specificHeaders.isEmpty()) {
            headers.addAll(specificHeaders);
        }
        return headers.toArray(new Header[0]);
    }

    private Map getData(String url) {
        LOGGER.info("Fetching Data using GET URL provided by User");
        Map map;
        List<Header> specificHeaders = new ArrayList<>();
        Header[] headers = generateHeaders(specificHeaders);
        try {
            RestResponseObject restResponseObject = restUtil.invokeHttpCall(HttpPost.METHOD_NAME,
                    null, url,RESPONSE_TYPE_MAP);
            StatusLine statusLine = restResponseObject.getStatusLine();
            if (statusLine != null) {
                LOGGER.error("Failed creating a batch with Http response code {} " +
                                "and reason {}",
                        statusLine.getStatusCode(),
                        statusLine.getReasonPhrase());
                throw new SnapDataException(ERR_FETCHING_DATA_MSG)
                        .withReason(String.format(COMMON_REASON,
                                statusLine.getStatusCode(),
                                statusLine.getReasonPhrase()))
                        .withResolution(COMMON_RESOLUTION);
            } else {
                map = restResponseObject.getBody();
            }
        } catch (IOException ioException) {
            LOGGER.error("Error while fetching Group Data", ioException);
            throw new ExecutionException(ioException, ERR_FETCHING_DATA_MSG)
                    .formatWith(ioException.getMessage());
        }
        return map;
    }

    @Override
    protected void process(Document document, String s) {
        LOGGER.debug("GET URL provided by User: ", getUrl());
        Map groupData = getData(getUrl());
        outputViews.write(documentUtility.newDocument(groupData));
    }
}