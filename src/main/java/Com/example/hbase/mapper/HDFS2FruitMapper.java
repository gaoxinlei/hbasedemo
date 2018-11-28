package Com.example.hbase.mapper;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 从hdfs导入到hbase的mapper
 */
public class HDFS2FruitMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] row = line.split("\t");
        String rowKey=row[0];
        String name=row[1];
        String color=row[2];

        byte[] rowKeyByteArray = Bytes.toBytes(rowKey);
        ImmutableBytesWritable rowKeyOutPut = new ImmutableBytesWritable(rowKeyByteArray);
        Put put = new Put(rowKeyByteArray);
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(name));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("color"),Bytes.toBytes(color));
        context.write(rowKeyOutPut,put);
    }
}
