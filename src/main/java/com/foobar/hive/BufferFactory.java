package com.foobar.hive;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.hadoop.conf.Configuration;

public class BufferFactory {

    public static RingBuffer<RowRecordFlyWeight> createRingBufferAndConsumer(
            int bufferSize, WriterContext writerContext) {
        RowRecordFlyWeightFactory recordFlyWeightFactory = new RowRecordFlyWeightFactory();
        RingBuffer<RowRecordFlyWeight> ringBuffer = RingBuffer.create(
                ProducerType.SINGLE, recordFlyWeightFactory,
                bufferSize, new BlockingWaitStrategy()
                );

        BatchEventProcessor<RowRecordFlyWeight> batchEventProcessor = new BatchEventProcessor<RowRecordFlyWeight>(
                ringBuffer, ringBuffer.newBarrier(), new RowRecordParquetWriter(writerContext)
        );
        ringBuffer.addGatingSequences(batchEventProcessor.getSequence());
        Thread consumerThread = new Thread(batchEventProcessor,"RowRecordConsumer");
        consumerThread.setDaemon(true);
        consumerThread.start();
        writerContext.setEventProcessor(batchEventProcessor);
        return ringBuffer;
    }

}
