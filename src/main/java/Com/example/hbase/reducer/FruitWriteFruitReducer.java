package Com.example.hbase.reducer;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * 将一个hbase表的部分列写到另一表。
 */
public class FruitWriteFruitReducer extends TableReducer<ImmutableBytesWritable,Put,NullWritable>{
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for(Put put:values){
            context.write(NullWritable.get(),put);
        }
    }
}
