package com.foobar.hive;

import com.lmax.disruptor.EventFactory;

/**
 * Created by hp on 6/14/14.
 */
public class RowRecordFlyWeightFactory implements EventFactory<RowRecordFlyWeight>{

    @Override
    public RowRecordFlyWeight newInstance() {
        return new RowRecordFlyWeight();
    }
}
