package Com.example.hbase.mapper;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 从hbase读取行键-row value的格式，并输出行键-put其中满足条件的列的mapper。
 */
public class FruitReadFruitMapper extends TableMapper<ImmutableBytesWritable, Put> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //默认的inputFormat一次读一行键。
        Put put = new Put(key.get());
        //每一个cell含行键，列族，列名和列值。
        Cell[] cells = value.rawCells();
        if (null != cells && cells.length > 0) {
            for (Cell cell : cells) {
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                //只要info列族的name列和color列的(cell)。
                if ("info".equals(Bytes.toString(family))) {
                    String column = Bytes.toString(qualifier);
                    if ("name".equals(column)||"color".equals(column)) {
                        put.add(cell);
                    }
                }
            }
            context.write(key, put);
        }

    }
}
