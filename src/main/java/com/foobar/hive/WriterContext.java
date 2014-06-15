package com.foobar.hive;

import com.lmax.disruptor.EventProcessor;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by hp on 6/14/14.
 */
public class WriterContext {
    private Configuration configuration;

    public Configuration getConfiguration() {
        return configuration;
    }

    public long getMaxRecordsInFile() {
        return maxRecordsInFile;
    }

    public String getBasePath() {
        return basePath;
    }

    public RowSchema getRowSchema() {
        return rowSchema;
    }

    private long maxRecordsInFile;
    private String basePath;
    private RowSchema rowSchema;

    public EventProcessor getEventProcessor() {
        return eventProcessor;
    }

    public void setEventProcessor(EventProcessor eventProcessor) {
        this.eventProcessor = eventProcessor;
    }

    private EventProcessor eventProcessor;

    public WriterContext(RowSchema schema, Configuration configuration,  String basePath, long maxRecordsInFile) {
        this.rowSchema = schema;
        this.configuration = configuration;
        this.maxRecordsInFile = maxRecordsInFile;
        this.basePath = basePath;
    }
}
