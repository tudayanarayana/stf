package com.snaplogic.snaps.stf;

import com.snaplogic.api.ConfigurationException;
import com.snaplogic.common.properties.SnapProperty;
import com.snaplogic.common.properties.builders.PropertyBuilder;
import com.snaplogic.snap.api.*;
import com.snaplogic.snap.api.capabilities.*;
import com.snaplogic.snaps.stf.utils.RestUtil;
import com.google.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.http.entity.mime.content.InputStreamBody;

import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.util.Map;

import static com.snaplogic.snaps.stf.Constants.FORWARD_SLASH;
import static com.snaplogic.snaps.stf.Constants.GENERATE_SCHEMA_FROM_FILE_URL;
import static com.snaplogic.snaps.stf.Constants.RESPONSE_TYPE_MAP;
import static com.snaplogic.snaps.stf.utils.RestUtil.RestResponseObject;

@Version()
@General(title = "Generate Schema From File", purpose = "Generate Schema From File")
@Inputs(max = 1, accepts = {ViewType.DOCUMENT})
@Outputs(max = 1, offers = {ViewType.DOCUMENT})
@Category(snap = SnapCategory.WRITE)
@PlatformFeature(coerceAndValidateExpressions = true)
public class GenerateSchemaFromFile extends SimpleSnap {
    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateSchemaFromFile.class);
    private static final String ENTITY_ID_FIELD = "entityId";
    private static final String ENTITY_ID_FIELD_LABEL = "Entity Id";
    private static final String ENTITY_ID_FIELD_DESCRIPTION = "Managed Service Entity Id";
    private static final String INPUT_FILE_FIELD = "inputFile";
    private static final String INPUT_FILE_FIELD_LABEL = "Input File";
    private static final String INPUT_FILE_FIELD_DESCRIPTION = "Managed Service Input File";
    private static final String BINARY_FILE_PATH = "binary.file_path";
    private static final String TENANT_ID_FIELD = "tenantId";
    private static final String TENANT_ID_FIELD_LABEL = "Tenant Id";
    private static final String TENANT_ID_FIELD_DESCRIPTION = "Managed Service Tenant Id";
    private static final String VERSION_FIELD = "version";
    private static final String VERSION_FIELD_LABEL = "Version";
    private static final String VERSION_FIELD_DESCRIPTION = "Managed Service Version";
    private String entityId;
    private String inputFile;
    private String fileName;
    private String tenantId;
    private String version;
    private URLConnection urlConnection;
    private static final String GET_URL_CONNECTION_ERROR = "Exception while opening URL connection";
    private static final String COMMON_RESOLUTION = "Ensure that the account credentials are correct and try again";
    private static final String FORMAT_SLDB = "sldb:///%s";
    private static final String INPUT_FILE = "inputFile";
    private static final String CREATE_ENTITY_EXCEPTION = "Exception while creating entity";
    @Inject
    private RestUtil restUtil;
    private static final String POSTING_DATA_MESSAGE_ERROR = "Unable to post data";
    private static final String COMMON_REASON = "HTTP code: %s, Reason: %s";

    @Override
    public void defineProperties(final PropertyBuilder propertyBuilder) {
        propertyBuilder.describe(ENTITY_ID_FIELD, ENTITY_ID_FIELD_LABEL, ENTITY_ID_FIELD_DESCRIPTION)
                .required()
                .add();
        propertyBuilder.describe(INPUT_FILE_FIELD, INPUT_FILE_FIELD_LABEL, INPUT_FILE_FIELD_DESCRIPTION)
                .required()
                .schemaAware(SnapProperty.DecoratorType.ACCEPTS_SCHEMA)
                .fileBrowsing()
                .dataLocationIdentifier(BINARY_FILE_PATH)
                .add();
        propertyBuilder.describe(TENANT_ID_FIELD, TENANT_ID_FIELD_LABEL, TENANT_ID_FIELD_DESCRIPTION)
                .required()
                .add();
        propertyBuilder.describe(VERSION_FIELD, VERSION_FIELD_LABEL, VERSION_FIELD_DESCRIPTION)
                .required()
                .add();
    }

    @Override
    public void configure(PropertyValues propertyValues) throws ConfigurationException {
        entityId = propertyValues.get(ENTITY_ID_FIELD);
        inputFile =  propertyValues.get(INPUT_FILE_FIELD);
        fileName = StringUtils.substringAfterLast(inputFile, FORWARD_SLASH);
        tenantId = propertyValues.get(TENANT_ID_FIELD);
        version = propertyValues.get(VERSION_FIELD);
    }

    private URLConnection getUrlConnection(URL url) throws IOException {
        urlConnection = url.openConnection();
        if (urlConnection == null) {
            errorViews.write((SnapDataException)
                    new SnapDataException(GET_URL_CONNECTION_ERROR)
                            .withResolution(COMMON_RESOLUTION));
        }
        return urlConnection;
    }

    private HttpEntity createEntity() {
        try {
            inputFile = String.format(FORMAT_SLDB, inputFile);
            URI build;
            build = new URIBuilder(new URI(inputFile)).build();
            String st = build.toString();
            MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create();
            URL url = new URL(st);
            urlConnection = getUrlConnection(url);
            urlConnection.connect();
            InputStream inputStream = urlConnection.getInputStream();
            InputStreamBody inputStreamBody;
            inputStreamBody = new InputStreamBody(inputStream, fileName);
            return multipartEntityBuilder.addPart(INPUT_FILE, inputStreamBody).build();
        } catch (IOException | SecurityException | URISyntaxException exception) {
            errorViews.write((SnapDataException)
                    new SnapDataException(exception, CREATE_ENTITY_EXCEPTION)
                            .withResolution(COMMON_RESOLUTION));
        }
        return null;
    }

    @Override
    protected void process(Document document, String s) {
        LOGGER.debug("Inside GenerateSchema.process() method");
        Map map;
        try {
            HttpEntity payload = createEntity();
            String generateSchemaFromFileUrl;
            generateSchemaFromFileUrl = String.format(GENERATE_SCHEMA_FROM_FILE_URL, entityId, tenantId, version);
            RestResponseObject restResponseObject = restUtil.invokeHttpCall(HttpPost.METHOD_NAME, payload,
                    generateSchemaFromFileUrl, RESPONSE_TYPE_MAP);
            StatusLine statusLine = restResponseObject.getStatusLine();
            if (statusLine != null) {
                LOGGER.error("Failed to generate schema with Http response code {} and reason {}",
                        statusLine.getStatusCode(),
                        statusLine.getReasonPhrase());
                throw new SnapDataException(POSTING_DATA_MESSAGE_ERROR)
                        .withReason(String.format(COMMON_REASON,
                                statusLine.getStatusCode(),
                                statusLine.getReasonPhrase()))
                        .withResolution(COMMON_RESOLUTION);
            } else {
                map = restResponseObject.getBody();
                outputViews.write(documentUtility.newDocument(map));
            }
        } catch (IOException e1) {
            throw new SnapDataException(e1, POSTING_DATA_MESSAGE_ERROR)
                    .withReason(e1.getMessage())
                    .withResolution(COMMON_RESOLUTION);
        }
        LOGGER.debug("End of GenerateSchema.process() method");
    }
}

