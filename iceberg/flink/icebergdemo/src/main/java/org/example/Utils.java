package org.example;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;

public class Utils {
    public static  final String WAREHOUSE_PATH = "hdfs://172.30.69.222:9000/user/hdfs/warehouse";

    public static HadoopCatalog getHadoopCatalog(String warehouseLocation){
        Configuration conf = new Configuration();
        HadoopCatalog hadoopCatalog = null;
        if(!Strings.isNullOrEmpty(warehouseLocation)){
            hadoopCatalog = new HadoopCatalog(conf, warehouseLocation);
        }else{
            hadoopCatalog = new HadoopCatalog(conf, WAREHOUSE_PATH);
        }
        return hadoopCatalog;
    }

    /**
     * Create tables in hadoop catalog
     * @param namespace
     * @param name
     * @param icebergSchema
     * @return
     */
    public static Table createHadoopTable(String loc,String namespace, String name, Schema icebergSchema){
        HadoopCatalog hadoopCatalog = getHadoopCatalog(loc);
        if(hadoopCatalog != null && !hadoopCatalog.tableExists(TableIdentifier.of(namespace, name))){
            Table table = hadoopCatalog.createTable(TableIdentifier.of(namespace, name), icebergSchema);
            return table;
        }
        return null;
    }
}
