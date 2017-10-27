package com.mapred.parser;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * <H3>JSONUtil</H3>
 *
 * @author TuTX1
 * @since Aug 11, 2014
 */
public class JsonUtil {

    private static final Logger LOG = Logger.getLogger(JsonUtil.class);
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static String[][] get2DArray(byte[] src) {
        ObjectMapper mapper = new ObjectMapper();

        String[][] result = null;
        try {
            result = mapper.readValue(src, String[][].class);
        } catch (JsonParseException e) {
            LOG.error(e, e.getCause());
        } catch (JsonMappingException e) {
            LOG.error(e, e.getCause());
        } catch (IOException e) {
            LOG.error(e, e.getCause());
        }

        return result;
    }

    public static String convertObjectToJson(Object obj) {
        String result = null;
        try {
            result = OBJECT_MAPPER.writeValueAsString(obj);
        } catch (JsonGenerationException e) {
            LOG.error(e, e.getCause());
        } catch (JsonMappingException e) {
            LOG.error(e, e.getCause());
        } catch (IOException e) {
            LOG.error(e, e.getCause());
        }
        return result;
    }

    public static <T> T convertJsonToObject(String json, Class<T> clazz) {
        T result = null;
        try {
            result = OBJECT_MAPPER.readValue(json.getBytes(), clazz);
        } catch (JsonGenerationException e) {
            LOG.error(e, e.getCause());
        } catch (JsonMappingException e) {
            LOG.error(e, e.getCause());
        } catch (IOException e) {
            LOG.error(e, e.getCause());
        }
        return result;
    }

}
