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
import org.apache.http.client.methods.HttpPost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.snaplogic.snaps.stf.Constants.*;
import static com.snaplogic.snaps.stf.Constants.ERR_FETCHING_DATA_MSG;

@Version()
@General(title = "IsContextTrue", purpose = "To check whether context is true or not")
@Inputs(max = 1, accepts = {ViewType.DOCUMENT})
@Outputs(min = 1, max = 1, offers = {ViewType.DOCUMENT})
@Category(snap = SnapCategory.WRITE)
public class IsContextTrue extends SimpleSnap {

    private static final Logger LOGGER = LoggerFactory.getLogger(IsEntityPartOfGroup.class);
    private static final String TENANT_ID_PROP = "tenantId";
    private static final String TENANT_ID_LABEL = "Tenant Id";
    private static final String TENANT_ID_DESC = "Tenant Id";
    private String tenantId;
    private static final String CONTEXT_ID_PROP = "contextId";
    private static final String CONTEXT_ID_LABEL = "Context Id";
    private static final String CONTEXT_ID_DESC = "Context Id";
    private String contextId;
    @Inject
    private RestUtil restUtil;

    @Override
    public void defineProperties(PropertyBuilder propertyBuilder) {
        propertyBuilder.describe(TENANT_ID_PROP, TENANT_ID_LABEL, TENANT_ID_DESC)
                .required()
                .add();
        propertyBuilder.describe(CONTEXT_ID_PROP, CONTEXT_ID_LABEL, CONTEXT_ID_DESC)
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
        contextId = propertyValues.get(CONTEXT_ID_PROP);
        if (StringUtils.isBlank(contextId)) {
            throw new ConfigurationException(ERR_SNAP_PROPERTY_MISSING_MSG)
                    .withReason(String.format(ERR_PROPERTY_MISSING_REASON,
                            CONTEXT_ID_LABEL))
                    .withResolution(String.format(ERR_PROPERTY_MISSING_RESOLUTION,
                            CONTEXT_ID_LABEL));
        }
    }

    private Map isContextTrue() {
        LOGGER.info("Start of IsContextTrue:isContextTrue() Method");
        Map map;
        String isContextTrueUrl;
        isContextTrueUrl = String.format(IS_CONTEXT_TRUE_URL, tenantId, contextId);
        try {
            RestUtil.RestResponseObject restResponseObject = restUtil.invokeHttpCall(HttpPost.METHOD_NAME,
                    null, isContextTrueUrl, RESPONSE_TYPE_STRING);
            StatusLine statusLine = restResponseObject.getStatusLine();
            if (statusLine != null) {
                LOGGER.error("Failed checking is context true or not with Http response code {} and reason {}",
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
            LOGGER.error("Error while checking is context true or not", ioException);
            throw new ExecutionException(ioException, ERR_FETCHING_DATA_MSG)
                    .formatWith(ioException.getMessage());
        }
        LOGGER.info("End of IsContextTrue:isContextTrue() Method");
        return map;
    }

    @Override
    protected void process(Document document, String s) {
        LOGGER.info("Start of IsContextTrue:process() Method");
        Map isEntityPartOfGroup = isContextTrue();
        outputViews.write(documentUtility.newDocument(isEntityPartOfGroup));
        LOGGER.info("End of IsContextTrue:process() Method");
    }
}
