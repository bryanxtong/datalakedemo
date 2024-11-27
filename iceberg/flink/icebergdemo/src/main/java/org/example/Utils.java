package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;

import java.util.HashMap;
import java.util.Map;

public class Utils {
    public static  final String WAREHOUSE_PATH = "hdfs://172.30.69.222:9000/user/hdfs/warehouse";

    enum CatalogType{
        HIVE,
        HADOOP,
        JDBC
    }

    public static Catalog getHadoopCatalog(){
        Configuration conf = new Configuration();
        HadoopCatalog hadoopCatalog = new HadoopCatalog(conf, WAREHOUSE_PATH);
        return hadoopCatalog;
    }

    public static Catalog getHiveCatalog(String catalogName){
        HiveCatalog catalog = new HiveCatalog();
        Map<String, String> hiveProperties = getHiveProperties();
        //It seems catalog name can be random string
        catalog.initialize(catalogName, hiveProperties);
        return catalog;
    }

    public static Map<String,String> getHiveProperties(){
        Map<String, String> hiveProperties = new HashMap<>();
        hiveProperties.put("warehouse", "hdfs://172.30.69.222:9000/user/hive/warehouse");
        hiveProperties.put("uri", "thrift://172.30.69.222:9083");
        return hiveProperties;
    }

    public static Map<String,String> getHadoopProperties(){
        Map<String, String> hiveProperties = new HashMap<>();
        hiveProperties.put("warehouse", "hdfs://172.30.69.222:9000/user/hdfs/warehouse");
        return hiveProperties;
    }

    /**
     * Create tables in hadoop catalog
     * @param namespace
     * @param name
     * @param icebergSchema
     * @return
     */
    public static Table createTable(CatalogType catalogType, String catalogName, String namespace, String name, Schema icebergSchema){
        Catalog catalog = null;
        if(catalogType.equals(CatalogType.HIVE)){
            catalog = getHiveCatalog(catalogName);
        }else if(catalogType.equals(CatalogType.HADOOP)){
            catalog = getHadoopCatalog();
        }

        if(catalog != null && !catalog.tableExists(TableIdentifier.of(namespace, name))){
            Table table = catalog.createTable(TableIdentifier.of(namespace, name), icebergSchema);
            return table;
        }

        return null;
    }
}
