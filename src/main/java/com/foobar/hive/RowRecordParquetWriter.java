package com.foobar.hive;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

public class RowRecordParquetWriter implements EventHandler<RowRecordFlyWeight>, LifecycleAware{
    private  SerDe serDe;
    private ObjectInspector objectInspector;
    private StructTypeInfo structTypeInfo;
    private ParquetWriter<ArrayWritable> writer;
    private WriterContext context;
    private final UUID myid = UUID.randomUUID();
    private int currentFileSequence;
    private long currentRecsInFile;


    private Path newFilePath() {
        return new Path(context.getBasePath(), myid.toString() + "-" + currentFileSequence);
    }

    private Path currentPath;

    private void closeFile() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        writer = null;
    }

    private ParquetWriter<ArrayWritable> getWriter(){
        if(currentRecsInFile >= context.getMaxRecordsInFile())
        {
            closeFile();
        }

        if(writer == null) {
            try {
                currentFileSequence ++;
                currentPath = newFilePath();
                writer = ParquetWriterHelper.createWriter(structTypeInfo,currentPath, context.getConfiguration());
                currentRecsInFile = 0;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return writer;
    }

    public RowRecordParquetWriter (WriterContext context) {
        this.context = context;
    }

    public  void init () {
        serDe = new ParquetHiveSerDe();
        objectInspector = new ObjectInspectorHelper(context.getRowSchema()).getObjectInspector();
        structTypeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromObjectInspector(objectInspector);
        currentFileSequence = 0;
    }

    public void flush() {
        closeFile();
    }

    @Override
    public void onEvent(RowRecordFlyWeight event, long sequence, boolean endOfBatch) throws Exception {
        Objects.requireNonNull(event.getRowRecord());
        long timestamp = event.getRowRecord().getTimeStamp();
        if (timestamp < 0) {
            if(timestamp == RowRecord.FLUSH_TIMESTAMP) {
                flush();
            }else if (timestamp == RowRecord.KILL_TIMESTAMP) {
                Thread.currentThread().stop();
            }
        }
        if(event.getRowRecord().getTimeStamp() == RowRecord.FLUSH_TIMESTAMP) {
            flush();
        }
        writeRecord(event.getRowRecord());
    }

    private void writeRecord(RowRecord rowRecord) {
        try {
            ArrayWritable serialized = (ArrayWritable) serDe.serialize(rowRecord, objectInspector);
            this.getWriter().write(serialized);
            currentRecsInFile++;
        } catch (SerDeException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onStart() {
        this.init();
    }

    @Override
    public void onShutdown() {
        flush();
    }
}
