package com.snaplogic.snaps.stf;

import com.google.inject.Inject;
import com.snaplogic.api.ConfigurationException;
import com.snaplogic.api.ExecutionException;
import com.snaplogic.common.properties.builders.PropertyBuilder;
import com.snaplogic.snap.api.*;
import com.snaplogic.snap.api.capabilities.*;
import com.snaplogic.snaps.stf.utils.RestUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.snaplogic.snaps.stf.Constants.*;
import static com.snaplogic.snaps.stf.Constants.ERR_FETCHING_DATA_MSG;

@Version()
@General(title = "GetAnalyticsQueryResults", purpose = "To get analytics query results")
@Inputs(max = 1, accepts = {ViewType.DOCUMENT})
@Outputs(min = 1, max = 1, offers = {ViewType.DOCUMENT})
@Category(snap = SnapCategory.WRITE)
public class GetAnalyticsQueryResults extends SimpleSnap {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetAnalyticsQueryResults.class);
    private static final String TENANT_ID_PROP = "tenantId";
    private static final String TENANT_ID_LABEL = "Tenant Id";
    private static final String TENANT_ID_DESC = "Tenant Id";
    private String tenantId;
    private static final String QUERY_ID_PROP = "queryId";
    private static final String QUERY_ID_LABEL = "Query Id";
    private static final String QUERY_ID_DESC = "Query Id";
    private String queryId;
    @Inject
    private RestUtil restUtil;

    @Override
    public void defineProperties(PropertyBuilder propertyBuilder) {
        propertyBuilder.describe(TENANT_ID_PROP, TENANT_ID_LABEL, TENANT_ID_DESC)
                .required()
                .add();
        propertyBuilder.describe(QUERY_ID_PROP, QUERY_ID_LABEL, QUERY_ID_DESC)
                .required()
                .add();
    }

    @Override
    public void configure(PropertyValues propertyValues) throws ConfigurationException {
        tenantId = propertyValues.get(TENANT_ID_PROP);
        if (StringUtils.isBlank(tenantId)) {
            throw new ConfigurationException(ERR_SNAP_PROPERTY_MISSING_MSG)
                    .withReason(String.format(ERR_PROPERTY_MISSING_REASON,
                            TENANT_ID_LABEL))
                    .withResolution(String.format(ERR_PROPERTY_MISSING_RESOLUTION,
                            TENANT_ID_LABEL));
        }
        queryId = propertyValues.get(QUERY_ID_PROP);
        if (StringUtils.isBlank(queryId)) {
            throw new ConfigurationException(ERR_SNAP_PROPERTY_MISSING_MSG)
                    .withReason(String.format(ERR_PROPERTY_MISSING_REASON,
                            QUERY_ID_LABEL))
                    .withResolution(String.format(ERR_PROPERTY_MISSING_RESOLUTION,
                            QUERY_ID_LABEL));
        }
    }

    private Map getAnalyticsQueryResults() {
        LOGGER.info("Start of GetAnalyticsQueryResults:getAnalyticsQueryResults() Method");
        Map map;
        String getAnalyticsQueryResultsUrl;
        getAnalyticsQueryResultsUrl = String.format(GET_ANALYTICS_QUERY_RESULTS_URL, tenantId, queryId);
        try {
            RestUtil.RestResponseObject restResponseObject = restUtil.invokeHttpCall(HttpGet.METHOD_NAME,
                    null, getAnalyticsQueryResultsUrl, RESPONSE_TYPE_MAP);
            StatusLine statusLine = restResponseObject.getStatusLine();
            if (statusLine != null) {
                LOGGER.error("Failed while getting analytics query results with Http response code {} and reason {}",
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
            LOGGER.error("Error while getting analytics query results", ioException);
            throw new ExecutionException(ioException, ERR_FETCHING_DATA_MSG)
                    .formatWith(ioException.getMessage());
        }
        LOGGER.info("End of GetAnalyticsQueryResults:getAnalyticsQueryResults() Method");
        return map;
    }

    @Override
    protected void process(Document document, String s) {
        LOGGER.info("Start of GetAnalyticsQueryResults:process() Method");
        Map analyticsQueryResults = getAnalyticsQueryResults();
        outputViews.write(documentUtility.newDocument(analyticsQueryResults));
        LOGGER.info("End of GetAnalyticsQueryResults:process() Method");
    }
}
