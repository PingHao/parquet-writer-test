package com.foobar.hive;

public class RowRecordFlyWeight {
    private RowRecord rowRecord;

    public void setRowRecord(RowRecord rowRecord) {
        this.rowRecord = rowRecord;
    }

    public  RowRecord getRowRecord() {
        return this.rowRecord;
    }
}
