package Com.example.hbase.runner;

import Com.example.hbase.mapper.HDFS2FruitMapper;
import Com.example.hbase.reducer.HDFS2FruitReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 从hdfs导入数据到hbase的runner。
 * 打好包后，使用命令
 * yarn jar hbase-demo-1.0-SNAPSHOT.jar  Com.example.hbase.runner.HDFS2FruitRunner
 * 即可将hdfs://192.168.233.128:9000/上的指定路径文件导入的hbase上的fruit_hdfs表中。
 *
 */
public class HDFS2FruitRunner extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception {
        //配置和job。
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(HDFS2FruitRunner.class);

        //输入路径。
        FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.233.128:9000/input_fruit/fruit.tsv"));
        //mapper
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setMapperClass(HDFS2FruitMapper.class);
        //reducer
        TableMapReduceUtil.initTableReducerJob("fruit_hdfs", HDFS2FruitReducer.class,job);
        job.setNumReduceTasks(1);
        boolean result = job.waitForCompletion(true);
        return result?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int result = ToolRunner.run(configuration, new HDFS2FruitRunner(), args);
        System.exit(result);
    }
}
