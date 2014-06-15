import com.foobar.hive.BufferFactory
import com.foobar.hive.RecordProducer
import com.foobar.hive.RowRecord
import com.foobar.hive.RowSchema
import com.foobar.hive.WriterContext
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

public class Test1 {

    private RowSchema userSessionSchema() {
        RowSchema rowSchema = new RowSchema();
        rowSchema << [
                timestamp: Long.class,
                username: String.class,
                logintype: Integer.class,
                numofprocess: Integer.class,
                cpuusage: Float.class
        ]

    }

    @Test public void testMain() {
        def writeContext = new WriterContext(userSessionSchema(),new Configuration(),"/tmp/rowrecordtest", 1000_000)
        def buffer =  BufferFactory.createRingBufferAndConsumer(
                2^10, writeContext
        )
        RecordProducer producer = new RecordProducer(buffer)

        def namelist = ['alice','bob','david']
        Random random = new Random()
        for (int i=0; i< 100_000; i++) {
            RowRecord record = new RowRecord();
            record.addAll([(long)i,namelist[i % namelist.size()], random.nextInt(10), 100, 2.5f])
            producer.out(record)
        }
        writeContext.eventProcessor.halt()
        sleep(5*1000)

    }
}
