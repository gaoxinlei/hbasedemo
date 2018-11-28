package Com.example.hbase.runner;

import Com.example.hbase.mapper.FruitReadFruitMapper;
import Com.example.hbase.reducer.FruitWriteFruitReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 从hbase表中导出部分列到另一个hbase表的runner。
 * 打包后使用命令
 * yarn jar hbase-demo-1.0-SNAPSHOT.jar  Com.example.hbase.runner.Fruit2FruitRunner
 * 即可将一个fruit表中的info列族的name，color一并导入到fruit_bk表。
 */
public class Fruit2FruitRunner extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(Fruit2FruitRunner.class);

        //配置job
        Scan scan = new Scan();
        scan.setCacheBlocks(false);//缓存块。
        scan.setCaching(100);//缓存列数

        TableMapReduceUtil.initTableMapperJob("fruit",scan,
                FruitReadFruitMapper.class, ImmutableBytesWritable.class, Put.class,job);
        TableMapReduceUtil.initTableReducerJob("fruit_bk", FruitWriteFruitReducer.class,job);

        job.setNumReduceTasks(1);

        boolean result = job.waitForCompletion(true);

        return result?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        int status = ToolRunner.run(configuration, new Fruit2FruitRunner(), args);
        System.exit(status);
    }
}
