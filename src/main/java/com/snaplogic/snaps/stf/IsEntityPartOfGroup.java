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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.snaplogic.snaps.stf.Constants.*;

@Version()
@General(title = "IsEntityPartOfGroup", purpose = "To check whether entity is part of group or not")
@Inputs(max = 1, accepts = {ViewType.DOCUMENT})
@Outputs(min = 1, max = 1, offers = {ViewType.DOCUMENT})
@Category(snap = SnapCategory.READ)
public class IsEntityPartOfGroup extends SimpleSnap {
    private static final Logger LOGGER = LoggerFactory.getLogger(IsEntityPartOfGroup.class);
    private static final String TENANT_ID_PROP = "tenantId";
    private static final String TENANT_ID_LABEL = "Tenant Id";
    private static final String TENANT_ID_DESC = "Tenant Id";
    private String tenantId;
    private static final String SCHEMA_ID_PROP = "schemaId";
    private static final String SCHEMA_ID_LABEL = "Schema Id";
    private static final String SCHEMA_ID_DESC = "Schema Id";
    private String schemaId;
    private static final String ENTITY_ID_PROP = "entityId";
    private static final String ENTITY_ID_LABEL = "Entity Id";
    private static final String ENTITY_ID_DESC = "Entity Id";
    private String entityId;
    @Inject
    private RestUtil restUtil;

    @Override
    public void defineProperties(PropertyBuilder propertyBuilder) {
        propertyBuilder.describe(TENANT_ID_PROP, TENANT_ID_LABEL, TENANT_ID_DESC)
                .required()
                .add();
        propertyBuilder.describe(SCHEMA_ID_PROP, SCHEMA_ID_LABEL, SCHEMA_ID_DESC)
                .required()
                .add();
        propertyBuilder.describe(ENTITY_ID_PROP, ENTITY_ID_LABEL, ENTITY_ID_DESC)
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
        schemaId = propertyValues.get(SCHEMA_ID_PROP);
        if (StringUtils.isBlank(schemaId)) {
            throw new ConfigurationException(ERR_SNAP_PROPERTY_MISSING_MSG)
                    .withReason(String.format(ERR_PROPERTY_MISSING_REASON,
                            SCHEMA_ID_LABEL))
                    .withResolution(String.format(ERR_PROPERTY_MISSING_RESOLUTION,
                            SCHEMA_ID_LABEL));
        }
        entityId = propertyValues.get(ENTITY_ID_PROP);
        if (StringUtils.isBlank(entityId)) {
            throw new ConfigurationException(ERR_SNAP_PROPERTY_MISSING_MSG)
                    .withReason(String.format(ERR_PROPERTY_MISSING_REASON,
                            ENTITY_ID_LABEL))
                    .withResolution(String.format(ERR_PROPERTY_MISSING_RESOLUTION,
                            ENTITY_ID_LABEL));
        }
    }

    private Map isEntityPartOfGroup() {
        LOGGER.info("Start of IsEntityPartOfGroup:isEntityPartOfGroup() Method");
        Map map;
        String isEntityPartOfGroupUrl;
        isEntityPartOfGroupUrl = String.format(IS_ENTITY_PART_OF_GROUP_URL, tenantId, schemaId, entityId);
        try {
            RestUtil.RestResponseObject restResponseObject = restUtil.invokeHttpCall(HttpGet.METHOD_NAME,
                    null, isEntityPartOfGroupUrl, RESPONSE_TYPE_MAP);
            StatusLine statusLine = restResponseObject.getStatusLine();
            if (statusLine != null) {
                LOGGER.error("Failed checking entity part of group or not with Http response code {} and reason {}",
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
            LOGGER.error("Error while checking entity part of group or not", ioException);
            throw new ExecutionException(ioException, ERR_FETCHING_DATA_MSG)
                    .formatWith(ioException.getMessage());
        }
        LOGGER.info("End of IsEntityPartOfGroup:isEntityPartOfGroup() Method");
        return map;
    }

    @Override

    protected void process(Document document, String s) {
        LOGGER.info("Start of IsEntityPartOfGroup:process() Method");
        Map isEntityPartOfGroup = isEntityPartOfGroup();
        outputViews.write(documentUtility.newDocument(isEntityPartOfGroup));
        LOGGER.info("End of IsEntityPartOfGroup:process() Method");
    }
}
