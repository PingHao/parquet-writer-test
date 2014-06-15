package com.foobar.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.io.ArrayWritable;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

import java.io.IOException;
import org.apache.hadoop.fs.Path;

public class ParquetWriterHelper {

    public static ParquetWriter<ArrayWritable> createWriter(StructTypeInfo structTypeInfo, Path path, Configuration configuration) throws IOException {
        final MessageType schema;
        try {
            schema = HiveSchemaConverter.convert(structTypeInfo.getAllStructFieldNames(),
                    structTypeInfo.getAllStructFieldTypeInfos());
        }catch (Throwable e) {
           e.printStackTrace();
            throw new IOException("WriterInstance error");
        }

        DataWritableWriteSupport ws = new DataWritableWriteSupport() {
            @Override
            public WriteContext init(Configuration configuration) {
                setSchema(schema, configuration);
                return super.init(configuration);
            }
        };
        ParquetWriter writer =  new ParquetWriter(path, ws , CompressionCodecName.SNAPPY,
                128 * 1024 * 1024, 1 * 1024 * 1024, 1 * 1024 * 1024,
                true, false, ParquetWriter.DEFAULT_WRITER_VERSION, configuration);
        return writer;
    }
}
