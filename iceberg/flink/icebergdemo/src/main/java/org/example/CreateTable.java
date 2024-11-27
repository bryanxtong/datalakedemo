package org.example;

import org.apache.iceberg.types.Types;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * CREATE CATALOG hadoop_catalog WITH (
 * 'type'='iceberg',
 * 'catalog-type'='hadoop',
 * 'warehouse'='hdfs://localhost:9000/user/hdfs/warehouse',
 * 'property-version'='1'
 * );
 * <p>
 * CREATE TABLE `hadoop_catalog`.`default`.`sample` (
 * id BIGINT COMMENT 'unique id',
 * data STRING,
 * PRIMARY KEY (`id`) NOT ENFORCED
 * );
 */
public class CreateTable {

    /**
     * create a sample table in hadoop catalog
     */
    public static void createSampleTable() {
        Set<Integer> identifierFieldIds = new HashSet<>();
        identifierFieldIds.add(1);
        List<Types.NestedField> nestedFields = Arrays.asList(Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "data", Types.StringType.get()));
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(nestedFields, identifierFieldIds);
        Utils.createTable(Utils.CatalogType.HADOOP, "hadoop_catalog", "default", "sample", icebergSchema);
    }

    public static void createSampleHiveTable() {
        Set<Integer> identifierFieldIds = new HashSet<>();
        identifierFieldIds.add(1);
        List<Types.NestedField> nestedFields = Arrays.asList(Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "data", Types.StringType.get()));
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(nestedFields, identifierFieldIds);
        Utils.createTable(Utils.CatalogType.HIVE, "hive_catalog", "default", "sample", icebergSchema);
    }

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "bryan");
        CreateTable.createSampleTable();
        CreateTable.createSampleHiveTable();
    }
}
