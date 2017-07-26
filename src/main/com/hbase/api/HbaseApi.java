package com.hbase.api;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.common.IOUtils;

/**
 * JUST FOR TEST
 * Created by tzq on 2017/7/20 0020.
 */
public class HbaseApi {
    public static HTable getHTableByName(String tableName) throws Exception{
        Configuration config= HBaseConfiguration.create();
        HTable table=new HTable(config,tableName);
        return table;
    }
    public static void getData() throws Exception{
        System.out.println("------------->");
        String tableName="user";
        HTable table=getHTableByName(tableName);
        Get get=new Get(Bytes.toBytes("10005"));
        // add column
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));

        //get data
        Result result=table.get(get);
        //key: rowkey +cf + c + version
        //value: value
        for (Cell cell:result.rawCells()){
            System.out.println(
                    "Family:" + Bytes.toString(CellUtil.cloneFamily(cell))+
                            "  Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell))+
                            "  Value:" + Bytes.toString(CellUtil.cloneValue(cell))
            );
        }
        table.close();
//
//        Family:infoQualifier:ageValue:26
//        Family:infoQualifier:nameValue:zhangsan
    }

    public static  void putData() throws Exception{
        String tableName="user";
        HTable table=getHTableByName(tableName);

        //Put 数据
        Put put=new Put(Bytes.toBytes("10005"));

        //add a column with value
        put.add(Bytes.toBytes("info"),
                Bytes.toBytes("name"),
                Bytes.toBytes("xiaoqiang"));
        put.add(Bytes.toBytes("info"),
                Bytes.toBytes("age"),
                Bytes.toBytes(27));
        put.add(Bytes.toBytes("info"),
                Bytes.toBytes("address"),
                Bytes.toBytes("shagnhai"));
        table.put(put);
        table.close();

        // flush 'user' 将memstore -》storefile
    }

    public static  void deleteData() throws Exception{
        String tableName="user";
        HTable table=getHTableByName(tableName);
        Delete delete=new Delete(Bytes.toBytes("10005"));
        //deleteColumns 删除所有版本数据，以下只删除最新版本的数据
        delete.deleteColumn(Bytes.toBytes("info"),Bytes.toBytes("address"));
        //删除整列数据
        delete.deleteFamily(Bytes.toBytes("info"));
        //删除后也可以合并表分区 compact 'user',优化参考：http://www.cnblogs.com/juncaoit/p/6170642.html
        table.delete(delete);
        table.close();

    }

    public static void testResultScanner()throws Exception{
        String tableName="user";
        HTable table=null;
        ResultScanner resultScanner=null;
        try {
            table = getHTableByName(tableName);
            Scan scan = new Scan(); //全表扫描，一般不能这么干，要加限制条件
            scan.setStartRow(Bytes.toBytes("10001")); //也可以放在scan的构造函数里面
            scan.setStopRow(Bytes.toBytes("10005")); //范围查询，包头不包尾

//            scan.addFamily()
//            scan.addColumn()

//比较牛叉的过滤器：PrefixFilter, pageFilter
//            scan.setFilter() //不要轻易使用，hbase默认使用rowkey查询很快，这里会使得查询变慢很多

//这两个是个很有用的参数，参考http://itindex.net/detail/44430-mr-hbase-scan
//            scan.setCacheBlocks(cacheBlocks);//为是否缓存块，默认缓存，我们分内存，缓存和磁盘，三种方式，一般数据的读取为内存->缓存->磁盘，当MR的时候为非热点数据，因此不需要缓存
//            scan.setCaching(caching);//每次从服务器端读取的行数，默认为配置文件中设置的值


             resultScanner = table.getScanner(scan);
            for (Result result:resultScanner){
                System.out.println(Bytes.toString(result.getRow()));
//                System.out.println(result);
                for (Cell cell:result.rawCells()){
                    System.out.println(
                            "Family:" + Bytes.toString(CellUtil.cloneFamily(cell))+
                                    "  Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell))+
                                    "  Value:" + Bytes.toString(CellUtil.cloneValue(cell))
                    );
                }
                System.out.println("----------------------------------------");

            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(resultScanner);//先关闭
            IOUtils.closeStream(table);//后关闭
        }
//        10001
//        keyvalues={10001/info:address/1500302359720/Put/vlen=8/mvcc=0, 10001/info:age/1500301997333/Put/vlen=2/mvcc=0, 10001/info:name/1500301939603/Put/vlen=8/mvcc=0, 10001/info:sex/1500302325882/Put/vlen=4/mvcc=0}
//        ----------------------------------------
//                10004
//        keyvalues={10004/info:address/1501036700214/Put/vlen=7/mvcc=0, 10004/info:age/1501036700214/Put/vlen=4/mvcc=0, 10004/info:name/1501036700214/Put/vlen=8/mvcc=0}
//        ----------------------------------------


//        10001
//        Family:info  Qualifier:address  Value:shanghai
//        Family:info  Qualifier:age  Value:26
//        Family:info  Qualifier:name  Value:zhangsan
//        Family:info  Qualifier:sex  Value:male
//                ----------------------------------------
//        10004
//        Family:info  Qualifier:address  Value:xinyang
//        Family:info  Qualifier:age  Value:   
//        Family:info  Qualifier:name  Value:zhangsan
//                ----------------------------------------
//        ok


    }

    public static void main(String[] args) throws Exception {
//        getData();
//        putData();
//        deleteData();
        testResultScanner();
        System.out.println("ok");
    }
}

//发现问题：从这里put上面的数据，数值类型转为字节后插入再取出为空，待解决