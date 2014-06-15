package com.foobar.hive;

import com.lmax.disruptor.RingBuffer;

/**
 * Created by hp on 6/14/14.
 */
public class RecordProducer {
    private RingBuffer<RowRecordFlyWeight> ringBuffer;
    public RecordProducer(RingBuffer<RowRecordFlyWeight> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void out(RowRecord record) {
        long seq = ringBuffer.next();
        ringBuffer.get(seq).setRowRecord(record);
        ringBuffer.publish(seq);
    }
}
