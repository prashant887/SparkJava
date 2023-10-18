package util;

import api.AnyPrimitive;
import api.ParseException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class RtsDataExtractorUtil {
    private static final String RTS_METADATA_HEADER_CONTENT_TYPE_KEY = "Content-Type";
    private static final String RTS_METADATA_HEADER_CONTENT_ENCODING_KEY = "Content-Encoding";

    public static final String RTS_METADATA_COLLECTION_ID_KEY = "collection-id";
    public static final String RTS_METADATA_HEADER_CONTENT_LENGTH = "Content-Length";
    public static final String RTS_METADATA_COLLECTOR_ID_KEY = "collector";
    public static final String RTS_METADATA_INSTANCE_ID_KEY = "instance";
    public static final String RTS_METADATA_RECEIVED_TIME_KEY = "received-time";
    public static final String RTS_METADATA_IS_EXTERNAL_KEY = "external";
    public static final String RTS_METADATA_DATA_URL_KEY = "data_url";
    public static final String RTS_METADATA_BUNDLE_FK_KEY = "bundle_fk";
    public static final String RTS_METADATA_COLLECTION_FK_KEY = "collection_fk";
    public static final String RTS_METADATA_REQUEST_SIZE_KEY = "total_request_size_bytes";
    public static final String RTS_METADATA_CHUNK_NUMBER_KEY = "chunk_number";
    public static final String RTS_METADATA_IS_LAST_CHUNK_KEY = "is_last_chunk";

    private static final Logger log = LoggerFactory.getLogger(RtsDataExtractorUtil.class);

    private static final JsonFactory jsonFactory;
    private static final ObjectMapper objectMapper;

    static {
        jsonFactory = new JsonFactory();
        jsonFactory.configure(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES, true);
        objectMapper = new ObjectMapper(jsonFactory);
    }


    public static class RemoteMessage {

        final private Metadata metadata;
        final InputStream uncompressedStream;

        public RemoteMessage(JsonNode metadata, InputStream uncompressedStream) {
            this.metadata = new Metadata(metadata);
            this.uncompressedStream = uncompressedStream;
        }

        public Metadata getMetadata() {
            return metadata;
        }


        public InputStream getPayload() {
            return uncompressedStream;
        }


        public static class Metadata {

            final private JsonNode metadata;
            private JSONObject metadataObject;

            Metadata(JsonNode metadata) {
                this.metadata = metadata;
            }

            private JSONObject getMetadataObject() {
                if (metadataObject == null) {
                    metadataObject = new JSONObject(metadata.toString());
                }
                return metadataObject;
            }

            /** {@inheritdoc} */
            public Map<String, AnyPrimitive> getClientMetadata() {
                return parseClientMetadata(getMetadataObject());
            }

            public Map<String, AnyPrimitive> getRtsMetaData() {
                return parseAllMetadata(getMetadataObject());
            }

            /**
             * @return Collection ID from RTS metadata, defaultCollectionId if null
             */
            public AnyPrimitive getCollectionId(String defaultCollectionId) throws IOException, ParseException {
                AnyPrimitive collectionId = getParameterValueFromMetadata(metadata, RTS_METADATA_COLLECTION_ID_KEY, false);
                if (collectionId == null && defaultCollectionId != null) {
                    collectionId = new AnyPrimitive(defaultCollectionId);
                }
                return collectionId;
            }

            /**
             * Extracts metadata fields out of the RTS JSON envelope
             *
             * @param json
             * @return metadata fields, not null, but can be empty
             * @throws JSONException
             */
            static Map<String, AnyPrimitive> parseClientMetadata(JSONObject json) throws JSONException {
                System.out.println("parseClientMetadata "+json);
                Map<String, AnyPrimitive> metadata = new HashMap<>();

                for (String key : JSONObject.getNames(json)) {
                    System.out.println(" parseClientMetadata key "+key);
                    if (StringUtils.isBlank(key)) {
                        log.warn("Blank key found in JSON. Skipping.");
                        continue;
                    }
                    if (key.equals("client")) {
                        return parseAllowedClientMetadata(json.getJSONObject(key));
                    } else if (json.optJSONObject(key) != null) {
                        JSONObject child = json.getJSONObject(key);
                        if (child.length() > 0) { // Ignore any empty JSON objects
                            metadata.putAll(parseClientMetadata(child));
                        }
                    }
                }
                return metadata;
            }

            static Map<String, AnyPrimitive> parseAllowedClientMetadata(JSONObject json) throws JSONException {
                Map<String, AnyPrimitive> metadata = new HashMap<>();
                String[] allowedMetadata = getAllowedClientMetadata();
                System.out.println("\n parseAllowedClientMetadata "+Arrays.toString(allowedMetadata));

                for (String key : allowedMetadata) {
                    if (json.has(key)) {
                        if (json.optJSONObject(key) != null) {
                            // Ignore since not expecting nested objects
                            log.warn("Nested object not expected parsing client meta data. It will be ignored: " + key);
                        } else if (json.optJSONArray(key) != null) {
                            // Ignore since not expecting an array
                            log.warn("Array not expected parsing client meta data. It will be ignored: " + key);
                        } else {
                            System.out.println("\n key = "+key+" json ="+json.get(key)+" Primitve ="+AnalyticsApiUtil.toAnyPrimitiveAdjustNumberType(json.get(key)));
                            metadata.put(key, AnalyticsApiUtil.toAnyPrimitiveAdjustNumberType(json.get(key)));
                        }
                    }
                }

                return metadata;
            }

            static Map<String, AnyPrimitive> parseAllMetadata(JSONObject json) throws JSONException {
                Map<String, AnyPrimitive> metadata = new HashMap<>();

                for (String key : JSONObject.getNames(json)) {
                    if (StringUtils.isBlank(key)) {
                        log.warn("Blank key found in JSON. Skipping.");
                        continue;
                    }
                    if (json.optJSONObject(key) != null) {
                        JSONObject child = json.getJSONObject(key);
                        if (child.length() > 0) { // Ignore any empty JSON objects
                            metadata.putAll(parseAllMetadata(child));
                        }
                    } else if (json.optJSONArray(key) != null) {
                        // Ignore since not expecting an array
                        log.warn("Array not expected parsing meta data. It will be ignored: " + key);
                    } else {
                        metadata.put(key, AnalyticsApiUtil.toAnyPrimitiveAdjustNumberType(json.get(key)));
                    }
                }
                return metadata;
            }

            private static String[] getAllowedClientMetadata() {
                String[] allowedMetadata = { "ua-browser", "ua-browser-version", "ua-os", "ua-os-version",
                        "country-from-ip", "state-from-ip", "_v", "vcsa_id" };

                return allowedMetadata;
            }

            /**
             * @return value for specified parameter from collection section of RTS metadata, may be <code>null</code>
             */
            static AnyPrimitive getParameterValueFromMetadata(JsonNode jsonNode, String parameterKey, boolean isHeader) {
                if (null != jsonNode) {
                    jsonNode = jsonNode.findValue("pa__metadata");
                    if (null != jsonNode) {
                        jsonNode = jsonNode.findValue("collection");
                        if (null != jsonNode && isHeader) {
                            jsonNode = jsonNode.findValue("headers");
                        }
                        if (null != jsonNode) {
                            jsonNode = jsonNode.findValue(parameterKey);
                        }
                    }
                }

                if (null != jsonNode) {
                    return new AnyPrimitive(jsonNode.toString().replaceAll("^\"|\"$", ""));
                } else {
                    return null;
                }
            }

        }

    }


    /**
     * Breaks a RTS message into metadata and payload.<br>
     * Some context: For every payload RTS adds metadata to the stream processing app (Kafka). This metadata is prepended
     * in the {@link InputStream} passed to the {@link Extractor} that processes the payload. The actual payload sent to
     * RTS follows immediately after the metadata object prepended by RTS; in other words the {@link InputStream}
     * contains malformed JSON, as it is in the form of two concatenated {@link String}s, each String being a JSON
     * object: <code>{...some metadata here}{actual payload}</code>, e.g.
     * <code> {"pa__metadata":{"something_here":"..."}}{"actual_payload_here":true}</code> (note that there is no comma
     * that separates the pa__metadata and actual_payload objects ) Current method extracts and separates metadata and
     * actual content of the payload. It is de-compressing the actual content of the payload in case it was sent
     * compressed.
     *
     * @return Pair<String, InputStream> where the {@link String} is payload metadata, and the {@link InputStream} is a
     *         the payload ready to be consumed (already decompressed, in case the actual payload was compressed over the
     *         wire)
     *
     **/
    public static RemoteMessage extractMetadataAndContentStream(InputStream messageStream)
            throws ParseException, IOException {
        // Use this custom Reader for reading one by one char metadata from the stream.
       // System.out.println("extractMetadataAndContentStream Message Stream:"+new String(messageStream.readAllBytes(), StandardCharsets.UTF_8));
        Reader reader = new SingleUtf8CharInputStreamReader(messageStream); //new InputStreamReader(messageStream);//
        System.out.println("extractMetadataAndContentStream Reader :"+reader);
        JsonParser parser = jsonFactory.createParser(reader);
        System.out.println("extractMetadataAndContentStream parser :"+parser);
        JsonNode metadata = objectMapper.readTree(parser);
        System.out.println("\n metadata extractMetadataAndContentStream :"+metadata);

        //System.out.println("extractMetadataAndContentStream Payload "+new String(messageStream.readAllBytes()));

        if (!isPayloadGzipped(metadata)) {
            System.out.println("Is Not Zipped extractMetadataAndContentStream");
            return new RemoteMessage(metadata, messageStream);
        }
        byte[] magic = new byte[2];
        if (messageStream.read(magic, 0, 2) < 2) {
            System.out.println(" extractMetadataAndContentStream : Magic 0 - 2 "+ Arrays.toString(magic));
            return new RemoteMessage(metadata, messageStream);
        }
        InputStream payloadStream = new SequenceInputStream(new ByteArrayInputStream(magic), messageStream);
        System.out.println(" extractMetadataAndContentStream : Magic 0 - 2 "+ Arrays.toString(magic));
        if (magic[0] == (byte) 0x1f && magic[1] == (byte) 0x8b) {
            payloadStream = new GZIPInputStream(payloadStream);
        }
        return new RemoteMessage(metadata, payloadStream);
    }

    private static boolean isPayloadGzipped(JsonNode metadataJson) {
        AnyPrimitive contentType = RemoteMessage.Metadata.getParameterValueFromMetadata(metadataJson,
                RTS_METADATA_HEADER_CONTENT_TYPE_KEY, true);
        AnyPrimitive contentEncoding = RemoteMessage.Metadata.getParameterValueFromMetadata(metadataJson,
                RTS_METADATA_HEADER_CONTENT_ENCODING_KEY, true);
        boolean isGzipped = false;

        if (contentType != null) {
            isGzipped = contentType.toString().contains("application/gzip");
        }
        if (contentEncoding != null) {
            isGzipped = isGzipped || contentEncoding.toString().contains("gzip");
        }

        return isGzipped;
    }
}
