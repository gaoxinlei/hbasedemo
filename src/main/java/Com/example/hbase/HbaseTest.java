package Com.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HbaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseTest.class);
    private static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.233.128");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

    }

    /**
     * 判断表名是否存在工具方法。
     * @param tableName
     * @return
     */
    public static boolean isTableExist(String tableName) throws IOException {
        //连接。
        HBaseAdmin admin = new HBaseAdmin(conf);

        return admin.tableExists(tableName);
    }

    public static void main(String[] args) throws IOException {
        LOGGER.info("表名student是否存在：{}",isTableExist("student"));
    }
}
