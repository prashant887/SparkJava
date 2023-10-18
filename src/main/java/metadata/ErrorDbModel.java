package metadata;

import api.AnyPrimitive;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import etl.Constants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import api.Record;
import util.AnalyticsApiUtil;
import util.RecordSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ErrorDbModel {

    private static final Logger log = LoggerFactory.getLogger(ErrorDbModel.class);

    private final Gson gson = new GsonBuilder().registerTypeAdapter(Record.class, new RecordSerializer()).create();

    public static Record generateErrorRecord(String sourceEntity, String errorCode, String errMsg, String srcRawData) {
        Map<String, AnyPrimitive> metrics =
                generateErrorRecordMetrics(sourceEntity, errorCode, errMsg, srcRawData);
        return new Record(Constants.ERROR_RECORDS_TABLE, metrics);
    }

    private static Map<String, AnyPrimitive> generateErrorRecordMetrics(String sourceEntity,
                                                                        String errorCode, String errMsg, String srcRawData) {
        Map<String, AnyPrimitive> metrics = new HashMap<>();
        metrics.put(Record.RECOMMENDED_PRIMARY_KEY,
                AnalyticsApiUtil.toAnyPrimitiveAdjustNumberType(UUID.randomUUID().toString()));
        if (!StringUtils.isBlank(sourceEntity)) {
            metrics.put(Constants.ERROR_RECORDS_SOURCE_ENTITY, AnalyticsApiUtil.toAnyPrimitiveAdjustNumberType(sourceEntity));
        }
        metrics.put(Constants.ERROR_RECORDS_ERROR_CODE, AnalyticsApiUtil.toAnyPrimitiveAdjustNumberType(errorCode));
        metrics.put(Constants.ERROR_RECORDS_ERROR_MESSAGE, AnalyticsApiUtil.toAnyPrimitiveAdjustNumberType(errMsg));
        metrics.put(Constants.ERROR_RECORDS_SOURCE_RAW_DATA,
                AnalyticsApiUtil.toAnyPrimitiveAdjustNumberType(srcRawData));
        return metrics;
    }

}
