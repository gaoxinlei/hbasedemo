package Com.example.hbase.reducer;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 从hdfs导入到hbase的reducer
 */
public class HDFS2FruitReducer extends TableReducer<ImmutableBytesWritable,Put,NullWritable>{
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for(Put put:values){
            context.write(NullWritable.get(),put);
        }
    }
}
