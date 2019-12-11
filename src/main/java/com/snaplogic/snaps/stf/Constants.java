package com.snaplogic.snaps.stf;

public class Constants {
    static final String ERR_SNAP_PROPERTY_MISSING_MSG = "Invalid Snap configuration";
    static final String ERR_PROPERTY_MISSING_REASON = "Property %s is null or missing";
    static final String ERR_PROPERTY_MISSING_RESOLUTION = "Ensure that the property %s has a valid non-null value";
    public static final String RESPONSE_TYPE_MAP = "map";
    public static final String RESPONSE_TYPE_STRING = "string";
    static final String ERR_FETCHING_DATA_MSG = "Unable to fetch data";
    static final String COMMON_REASON = "HTTP code: %s, Reason: %s";
    static final String COMMON_RESOLUTION = "Ensure that the account credentials are correct and try again";
    static final String FORWARD_SLASH = "/";
    static final String GENERATE_SCHEMA_FROM_FILE_URL = "http://qa.mappingservice.gaian.com/generate/schema/file?" +
                                                        "entityId=%s&tenantId=%s&version=%s";
    static final String MAP_ENTITIES_FROM_FILE_URL = "http://qa.mappingservice.gaian.com/entity/mapping/file?" +
                                                     "mappingId=%s";
    static final String IS_ENTITY_PART_OF_GROUP_URL = "http://192.168.28.37:8282/TFW//v1/%s/entity/groups/%s/%s";
    static final String IS_CONTEXT_TRUE_URL = "http://192.168.28.37:8282/TFW/v1/%s/context/evaluation/%s";
    static final String GET_ANALYTICS_QUERY_RESULTS_URL = "http://192.168.28.37:8282/TFW/v1/%s/analytics/query/data/%s";
}
