package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Utils {
    public static  final String WAREHOUSE_PATH = "hdfs://172.30.69.222:9000/user/hdfs/warehouse";

    public enum CatalogType{
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

    /**
     * get the avro schema from the pre-defined ones
     * @param name
     * @return
     * @throws URISyntaxException
     * @throws IOException
     */
    public static org.apache.avro.Schema getAvroSchema(String name) throws URISyntaxException, IOException {
        URL resource = Utils.class.getClassLoader().getResource("avro/"+ name);
        List<String> lines = Files.readAllLines(Paths.get(resource.toURI()));
        StringBuilder schemaStr = new StringBuilder();
        for(String line : lines){
            schemaStr.append(line);
        }
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        org.apache.avro.Schema schema = parser.parse(schemaStr.toString());
        return schema;
    }

    /**
     * Read json data line from resources/data/batch_1.json
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    public static List<String> readJsonLines(String fileName) throws IOException, URISyntaxException {
        URL resource = Utils.class.getClassLoader().getResource("data/" + fileName);
        return Files.readAllLines(Paths.get(resource.toURI()));
    }
}
