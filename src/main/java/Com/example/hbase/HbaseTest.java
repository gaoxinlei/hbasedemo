package Com.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

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
    private static boolean isTableExist(String tableName) throws IOException {
        //连接。
        HBaseAdmin admin = new HBaseAdmin(conf);

        return admin.tableExists(tableName);
    }

    private static void createTableIfNotExist(String tableName,String... cfs) throws IOException {
        if(isTableExist(tableName)){
            LOGGER.info("表名:{}已经存在",tableName);
        }else{
            Connection connection = ConnectionFactory.createConnection(conf);
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for(String cf:cfs){
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            connection.getAdmin().createTable(descriptor);
            connection.close();
            LOGGER.info("表:{}创建成功",tableName);
        }
    }
    private static void deleteIfExists(String tableName) throws IOException {
        if(isTableExist(tableName)){
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            admin.close();
            connection.close();
            LOGGER.info("成功删除了表：{}",tableName);
        }else{
            LOGGER.info("表：{}不存在",tableName);
        }
    }

    private static void addRowData(String tableName,String routeKey,
                                   String columnFamily,String column,String value) throws IOException {
        if(isTableExist(tableName)){
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(routeKey));
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
            table.put(put);
            table.close();
            connection.close();
            LOGGER.info("成功向表:{}中插入了数据",tableName);
        }else{
            LOGGER.info("表:{}不存在，无法插入数据",tableName);
        }
    }

    /**
     * 删除多行。
     * @param rows 行key
     */
    private static void deleteCols(String tableName,String ...rows) throws IOException {
        if(null!=rows&&rows.length>0){
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            List<Delete> deletes = new ArrayList<>();
            for(String row:rows){
                Delete delete = new Delete(Bytes.toBytes(row));
                deletes.add(delete);
            }
            table.delete(deletes);
            table.close();
            connection.close();
        }
    }

    /**
     * 查整行键匹配的所有列族的列值。
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    private static void listRow(String tableName,String rowKey) throws IOException {
        if(isTableExist(tableName)){
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            //可以用gets批量查询。
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            Cell[] cells = result.rawCells();
            if(null!=cells&&cells.length>0){
                for(Cell cell:cells){
                    String routeKey = Bytes.toString(CellUtil.cloneRow(cell));
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    long timestamp = cell.getTimestamp();
                    LOGGER.info("路由键：{},列族名:{},列名:{},值:{},时间:{}",
                            rowKey,family,columnName,value,timestamp);

                }
            }
            table.close();
            connection.close();
        }else{
            LOGGER.info("表:{}不存在",tableName);
        }
    }

    /**
     * 查指定的列族下的列名。
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param qualifyName
     * @throws IOException
     */
    private static void listRow(String tableName,String rowKey,String familyName,String qualifyName) throws IOException {
        if(isTableExist(tableName)){
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            //可以用gets批量查询。
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(qualifyName));
            Result result = table.get(get);
            Cell[] cells = result.rawCells();
            if(null!=cells&&cells.length>0){
                for(Cell cell:cells){
                    String routeKey = Bytes.toString(CellUtil.cloneRow(cell));
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    long timestamp = cell.getTimestamp();
                    LOGGER.info("路由键：{},列族名:{},列名:{},值:{},时间:{}",
                            rowKey,family,columnName,value,timestamp);

                }
            }
            table.close();
            connection.close();
        }else{
            LOGGER.info("表:{}不存在",tableName);
        }
    }
    public static void main(String[] args) throws IOException {
//        LOGGER.info("表名student是否存在：{}",isTableExist("student"));
//        createTableIfNotExist("teacher","info","backup");
//        deleteIfExists("teacher");
//        addRowData("student","1003","info","age","28");
//        addRowData("student","1003","info","name","leiou");
//        addRowData("student","1003","info","area","0451");
//        listRow("student","1002");
        listRow("student","1001","info","name");
    }
}
