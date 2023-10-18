package cdf;

import api.*;
import etl.StandardEtlError;
import metadata.ErrorDbModel;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.AnalyticsApiUtil;
import util.RtsDataExtractorUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class StreamingJsonExtractor implements Extractor {

    private static final Logger log = LoggerFactory.getLogger(StreamingJsonExtractor.class);

    private static final String ID_KEY = "@id";
    private static final String TYPE_KEY = "@type";
    private static final String TABLE_KEY = "@table";

    private static final String SYSTEM_COLUMN_ARRAYS = "_1";
    private static final String PA__ARRAY_INDEX_PROPERTY = "pa__array_index";
    private static final int RAW_SRC_MAX_LEN = 300;


    @Override
    public List<Record> extract(InputStream fileStream, InputContext fileContext) throws ParseException, IOException {
        List<Record> records = new ArrayList<>();
        //workaround org.json classloading issues
        String content = null;
        try {
            System.out.println(" in extract: Extract");
            RtsDataExtractorUtil.RemoteMessage remoteMessage = RtsDataExtractorUtil.extractMetadataAndContentStream(fileStream);
            Map<String, AnyPrimitive> clientMetaData = remoteMessage.getMetadata().getClientMetadata();
            System.out.println("\n clientMetaData :\n "+clientMetaData+" \n "+remoteMessage.getMetadata());
            System.out.println("\n getPayload :\n"+remoteMessage.getPayload());

            try (InputStream in = remoteMessage.getPayload()) {
                content = IOUtils.toString(in, "UTF-8");
            } catch (Exception e) {
                log.error("Exception thrown loading payload content: " + e.getMessage() + " Context: " + fileContext, e);
                return records;
            }
            JSONTokener tokener = new JSONTokener(content);

            System.out.println("\n Orig content: \n"+content);

            System.out.println("\n Orig tokener: \n"+tokener);

            while (tokener.skipTo('{') != 0) {
                System.out.println("\n tokener: \n"+tokener);
                JSONObject token = new JSONObject(tokener);
                System.out.println("\n token: \n"+token);
                records.addAll(parseObject(null, null, "", token, clientMetaData, fileContext.getFilePath()));
            }


        }catch (JSONException e) {
            System.out.println(" in extract: Extract Exception "+e.getMessage());
            log.warn("Exception thrown parsing JSON: " + e.getMessage() + " Context: " + fileContext, e);
            records.add(genErrorRecord(e, null, IOUtils.toString(fileStream, "UTF-8")));
        }
       /* for (Record record:records){
            System.out.println("getMetrics :"+record.getMetrics());
            System.out.println("getRelatedRecords :"+record.getRelatedRecords());

        }

        */
        return records;

    }

    private static Record genErrorRecord(Exception e, String sourceEntity, String json) {
        String errMsg = e.getMessage() != null ? e.getMessage() : "No error message available";
        return ErrorDbModel.generateErrorRecord(sourceEntity, StandardEtlError.CdfJsonParsingFailure.getErrorCode(),
                errMsg, truncateJson(json));
    }

    private static String truncateJson(String json) {
        return json.length() <= RAW_SRC_MAX_LEN ? json : json.substring(0, RAW_SRC_MAX_LEN) + "...";
    }

    private static List<Record> parseObject(String parentId, String parentType, String name, JSONObject json,
                                            Map<String, AnyPrimitive> metadata, String kafkaInfo) throws ParseException {
        List<Record> records = new ArrayList<>();

        try {
            String id = json.has(ID_KEY) ? json.getString(ID_KEY) : UUID.randomUUID().toString();

            System.out.println("\n parseObject Id\n"+id);

            String type = json.has(TABLE_KEY) ? json.getString(TABLE_KEY) : json.has(TYPE_KEY) ? json.getString(TYPE_KEY)
                    : (parentType != null) ? String.format("%s_%s", parentType, name) : null;
            if (type == null) {
                log.warn("Could not determine @type of object");
                throw new IllegalArgumentException("Could not determine type of object: " + id);
            }
            System.out.println("\n parseObject type\n"+type);

            System.out.println("\n parseObject parentId\n "+parentId+" \n parentType "+parentType);

            Map<String, RelatedRecord> relations = new HashMap<>();
            if (parentId != null) {
                relations.put(String.format("%s__fk", parentType), new RelatedRecord(parentId, parentType));
            }

            System.out.println("\n parseObject relations\n"+relations);


            Map<String, AnyPrimitive> metrics = new HashMap<>(metadata);

            for (String key : JSONObject.getNames(json)) {
                System.out.println("\n parseObject key "+key);
                if (key.startsWith("@")) {
                    continue;
                }

                if (StringUtils.isBlank(key)) {
                    log.warn("Blank key found in JSON. Skipping.");
                    continue;
                }

                if (json.optJSONObject(key) != null) {
                    JSONObject child = json.getJSONObject(key);
                    System.out.println(" getJSONObject "+key+" \n child \n"+child);

                    if (child.length() > 0) { // Ignore any empty JSON objects
                        System.out.println("Child lenght "+child.length());
                        System.out.println(String.format("id = %s type=%s key=%s child=%s metadata=%s kafkainfo=%s",id, type, key, child, metadata, kafkaInfo));
                        records.addAll(parseObject(id, type, key, child, metadata, kafkaInfo));
                    }
                } else if (json.optJSONArray(key) != null) {

                    JSONArray jarray = json.getJSONArray(key);
                    System.out.println(" getJSONArray "+key+" \n jarray \n"+jarray);

                    records.addAll(parseArray(id, type, key, jarray, metadata, kafkaInfo));
                } else {
                    System.out.println("\n parseObject-metrics "+key+" "+key.intern()+ " AnalyticsApiUtil "+AnalyticsApiUtil.toAnyPrimitiveAdjustNumberType(json.get(key)));
                    metrics.put(key.intern(), AnalyticsApiUtil.toAnyPrimitiveAdjustNumberType(json.get(key)));
                }
            }

            // Records store their ID in the metrics map using the "ID" key.
            System.out.println("\n parseObject_id_metrics Before "+metrics);

            metrics.put(Record.RECOMMENDED_PRIMARY_KEY, AnalyticsApiUtil.toAnyPrimitiveAdjustNumberType(id));
            System.out.println("\n parseObject_id_metrics After "+metrics);
            System.out.println("\n type "+type+" metrics "+metrics+" relations "+relations);
            records.add(new Record(type, metrics, relations));
        } catch (Exception e) {
            log.warn("Exception thrown parsing JSON: " + e.getMessage(), e);
            records.add(genErrorRecord(e, json.has(TABLE_KEY) ? json.getString(TABLE_KEY) : json.optString(TYPE_KEY), String.valueOf(json)));
        }
        return records;
    }
    private static List<Record> parseArray(String parentId, String parentType, String name, JSONArray jarray,
                                           Map<String, AnyPrimitive> metadata, String kafkaInfo) throws ParseException {
        System.out.println("=======parseArray=========== parentId = "+parentId+" parentType "+parentType+" jarray "+jarray
        +" metadata "+metadata+" kafkaInfo "+kafkaInfo
        );
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < jarray.length(); i++) {
            JSONObject json = jarray.optJSONObject(i);
            System.out.println("parseArray json "+i+" - "+json);
            if (json != null) {
                System.out.println("parseArray Not Null Json");
                records.addAll(parseObject(parentId, parentType, name, json, metadata, kafkaInfo));
            } else if (jarray.optJSONArray(i) != null) {
                System.out.println("\n parseArray json Nested Array "+i+" - "+jarray.optJSONArray(i));

                log.warn(String.format(
                        "Nested array in array structure detected - parentID:%s, parentType:%s,propertyName:%s. Ignoring incorrect value",
                        parentId, parentType, name));
            } else {
                Object simpleValue = jarray.opt(i);
                System.out.println("\n parseArray json simpleValue Array "+i+" - "+simpleValue);
                if (simpleValue != null) {
                    records.add(buildRecordForPrimitiveArrayValue(parentId, parentType, name, simpleValue, i));
                }
            }
        }
        return records;
    }

    private static Record buildRecordForPrimitiveArrayValue(String parentId, String parentType, String name,
                                                            Object value, int arrayIndex) {
        String newRecordID =
                new StringBuilder(parentId).append('_').append(name).append('_').append(arrayIndex).toString(); // parentId_property_offset
        String type = String.format("%s_%s", parentType, name);

        System.out.println("\n  buildRecordForPrimitiveArrayValue - newRecordID = "+newRecordID+" type = "+type);

        System.out.println("\n buildRecordForPrimitiveArrayValue - PA__ARRAY_INDEX_PROPERTY = "+PA__ARRAY_INDEX_PROPERTY+" arrayIndex "+arrayIndex+" AnalyticsApiUtil "+AnalyticsApiUtil.toAnyPrimitiveAdjustNumberType(arrayIndex));


        System.out.println("\n buildRecordForPrimitiveArrayValue - RECOMMENDED_PRIMARY_KEY = "+Record.RECOMMENDED_PRIMARY_KEY+" newRecordID "+newRecordID+" AnalyticsApiUtil "+AnalyticsApiUtil.toAnyPrimitiveAdjustNumberType(newRecordID));

        System.out.println("\n buildRecordForPrimitiveArrayValue - SYSTEM_COLUMN_ARRAYS = "+SYSTEM_COLUMN_ARRAYS+" arrayIndex "+value+" AnalyticsApiUtil "+AnalyticsApiUtil.toAnyPrimitiveAdjustNumberType(value));

        Map<String, AnyPrimitive> metrics = new HashMap<>();
        metrics.put(PA__ARRAY_INDEX_PROPERTY, AnalyticsApiUtil.toAnyPrimitiveAdjustNumberType(arrayIndex));
        metrics.put(Record.RECOMMENDED_PRIMARY_KEY, AnalyticsApiUtil.toAnyPrimitiveAdjustNumberType(newRecordID));
        metrics.put(SYSTEM_COLUMN_ARRAYS, AnalyticsApiUtil.toAnyPrimitiveAdjustNumberType(value));

        System.out.println("\n buildRecordForPrimitiveArrayValue - metrics = "+metrics);


        Map<String, RelatedRecord> relations = new HashMap<>();
        relations.put(String.format("%s__fk", parentType), new RelatedRecord(parentId, parentType));

        System.out.println("\n buildRecordForPrimitiveArrayValue - relations = "+relations);

        return new Record(type, metrics, relations);
    }

}
