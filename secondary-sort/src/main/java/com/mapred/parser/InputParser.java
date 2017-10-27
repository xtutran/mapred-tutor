package com.mapred.parser;

import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class InputParser {
    private static final String COLUMNS_KEY = "com.mapred.columns";
    private static final String TIMEWINDOWS_KEY = "com.mapred.timewindows";
    private static final String DATETIMEFORMAT_KEY = "com.mapred.datetimeformat";

    private DateTimeFormatter formater;
    private int timeWindows;
    private Map<String, Integer> columnMapping;
    private String[] values;

    private static final InputParser instance = new InputParser();

    private InputParser() {
    }

    public static InputParser getInstance() {
        return instance;
    }

    /**
     * made for unit test
     *
     * @param conf
     */
    public void loadLocalConf(Properties conf) {
        if (conf == null) {
            return;
        }

        this.formater = DateTimeFormat.forPattern(conf.getProperty(DATETIMEFORMAT_KEY, "yyyyMMddHHmmss"));
        this.timeWindows = Integer.parseInt(conf.getProperty(TIMEWINDOWS_KEY, "0"));
        this.loadColumnMapping(conf.getProperty(COLUMNS_KEY, "").split(","));
    }

    /**
     * made for run the job
     *
     * @param conf
     */
    public void loadHadoopConf(Configuration conf) {
        if (conf == null) {
            return;
        }

        this.formater = DateTimeFormat.forPattern(conf.get(DATETIMEFORMAT_KEY, "yyyyMMddHHmmss"));
        this.timeWindows = conf.getInt(TIMEWINDOWS_KEY, 0);
        this.loadColumnMapping(conf.getStrings(COLUMNS_KEY));
    }

    /**
     * create column - index mapping
     *
     * @param columns
     */
    private void loadColumnMapping(String[] columns) {
        if (columns == null) {
            return;
        }

        if (columnMapping == null) {
            columnMapping = new HashMap<String, Integer>();
        }

        int i = 0;
        for (String col : columns) {
            if (col.isEmpty())
                continue;
            columnMapping.put(col.trim(), i++);
        }
    }

    public void parse(String inputLine) {
        values = StringUtils.split(inputLine, ';');
    }

    public boolean containsKey(String key) {
        return columnMapping.containsKey(key);
    }

    public String getValue(String key) {
        Integer columnIndex = columnMapping.get(key);

        if (columnIndex == null || columnIndex > values.length || columnIndex < 0) {
            return null;
        }

        return values[columnIndex];
    }

    public Map<String, Integer> getColumnMapping() {
        return columnMapping;
    }

    public String[] getValues() {
        return values;
    }

    public boolean existOnTimeRange(String dateTimeString) {
        DateTime dateTime = formater.parseDateTime(dateTimeString);
        return Days.daysBetween(dateTime, new DateTime()).getDays() <= timeWindows;
    }

    public void cleanup() {
        this.values = null;
        System.gc();
    }
}
