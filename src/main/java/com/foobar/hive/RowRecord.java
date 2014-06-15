package com.foobar.hive;

import java.util.ArrayList;
import java.util.List;

public class RowRecord extends ArrayList {
    public final  static long INVALID_TIMESTAMP = -10L;
    public final  static long FLUSH_TIMESTAMP = -1L;
    public final  static long KILL_TIMESTAMP = -2L;

    public final static RowRecord FlushRecord;
    public final static RowRecord KillRecord;

    static {
        FlushRecord = new RowRecord() {{
            add(FLUSH_TIMESTAMP);
        }
        };
        KillRecord = new RowRecord() {{
            add(KILL_TIMESTAMP);
        }
        };
    }

    public long getTimeStamp(){
        return size() > 0 ? (Long)get(0) : INVALID_TIMESTAMP;
    }
}
