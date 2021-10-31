package algorithm;

import newick.TestPhylonet;
import properties.Configs;
import mapper.QuartetToTreeTablePartitionMapper;
import mapper.StringToTaxaTableMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import reducer.TaxaTableReducer;
import reducer.TreeTableReducer;
import structure.TaxaTable;
import structure.TreeTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;


public class Distributer {
    public static String runFunctions(String inputFileName, String outputFileName) {
        Dataset<Row> sortedQtDf = readFileInDf(inputFileName);
        TaxaTable taxaTable = initialiZeTaxaTable(sortedQtDf);
        Dataset<Row> taggedDf = groupTaxaAndTagData(sortedQtDf, taxaTable);
        TreeTable treeTable = partitionDataAndRun(taggedDf, taxaTable);
        String centralizedRunTree = runCentalized(sortedQtDf);
        System.out.println("centralizedRunTree: "+centralizedRunTree);
        System.out.println("distributedRunTree: "+treeTable.getTree());
        return treeTable.getTree();
    }


    public static String runCentalized(Dataset<Row> sortedWqDf) {
        sortedWqDf = sortedWqDf.withColumn("weightedQuartet", concat(col("value"), lit(" "), col("count")));
        List<String> qtList = sortedWqDf.select("weightedQuartet").as(Encoders.STRING()).collectAsList();
        return new wQFMRunner().runDevideNConquer(qtList);
    }

    public static Dataset<Row> readFileInDf(String inputFileName) {
        return Configs.SPARK.read().option("header", "true")
                .csv(Configs.HDFS_PATH + "/" + inputFileName);
        // .orderBy(desc("count"));
    }

    public static TaxaTable initialiZeTaxaTable(Dataset<Row> sortedWqDf) {
        TaxaTable taxaTable = sortedWqDf
                .map(new StringToTaxaTableMapper(), Encoders.bean(TaxaTable.class))
                .reduce(new TaxaTableReducer());

        System.out.println("Final Taxa Table: " + taxaTable.toString());
        return taxaTable;
    }

    public static Dataset<Row> groupTaxaAndTagData(Dataset<Row> sortedWqDf, TaxaTable taxaTable) {
        // Map<String, ArrayList<String>> taxaPartitionMap = TaxaPartition.partitionTaxaListByCombination(taxaTable.TAXA_LIST);
        Map<String, ArrayList<String>> taxaPartitionMap = TaxaPartition.partitionTaxaListByTaxaTable(taxaTable.TAXA_PARTITION_LIST);
        //Print partitionMap
        for (Map.Entry<String, ArrayList<String>> partition : taxaPartitionMap.entrySet()) {
            System.out.println(partition);
        }

        //t's because, List returned by subList() method is an instance of 'RandomAccessSubList' which is not serializable.
        // Therefore, you need to create a new ArrayList object from the list returned by the subList().
        UserDefinedFunction tagger = udf(
                (String qtStr) -> TaxaPartition.getTag(qtStr, taxaPartitionMap), DataTypes.StringType
        );
        Dataset<Row> taggedQtDf = sortedWqDf.withColumn("tag", tagger.apply(col("value")));
        taggedQtDf.groupBy(col("tag")).count().show(false);
        return taggedQtDf;
    }

    public static TreeTable partitionDataAndRun(Dataset<Row> taggedQtDf, TaxaTable taxaTable) {
        Dataset<Row> partitionedDf = taggedQtDf
                .withColumn("weightedQuartet", concat(col("value"), lit(" "), col("count")))
                // .filter(col("tag").notEqual("UNDEFINED"))
                .orderBy("tag"); // orderBy partitioned unique data to same partition

        // TaxaPartition.getPartitionDetail(partitionedDf);
        System.out.println("NumPartitions: " + partitionedDf.javaRDD().getNumPartitions());

        Dataset<TreeTable> treeTableDf = partitionedDf.select("weightedQuartet", "tag", "count")
                .mapPartitions(new QuartetToTreeTablePartitionMapper(), Encoders.bean(TreeTable.class))
                .cache() //avoid lazy execution i.e. running twice
                .orderBy(desc("support"))
                .filter(col("tree").notEqual("<NULL>"));
        // .toDF();

        TreeTable finalTreeTable = treeTableDf.reduce(new TreeTableReducer(taxaTable.TAXA_LIST));
        treeTableDf.show(false);
        System.out.println("Final tree " + finalTreeTable);

        return finalTreeTable;
    }
}
/* divide list of taxa into n partitions;
 * the taxa in each partitions canbe decided by finding the most quuartets that can be included by such partitions
 * which can be done by creating a list of partitions searching for better list by looping over each quartets
 * https://stackoverflow.com/questions/68386699/java-spark-sql-udf-with-complex-input-parameter
 * */

/* https://stackoverflow.com/questions/44878294/why-spark-dataframe-is-creating-wrong-number-of-partitions
 * repartition(columnName) per default creates 200 partitions (more specific, spark.sql.shuffle.partitions partitions),
 * no matter how many unique values of col1 there are. If there is only 1 unique value of col1, then 199 of the partitions are empty.
 * On the other hand, if you have more than 200 unique values of col1, you will will have multiple values of col1 per partition.
 * If you only want 1 partition, then you can do repartition(1,col("col1")) or just coalesce(1).
 * But not that coalesce does not behave the same in the sense that coalesce me be moved further up in your code
 * such that you may loose parallelism (see How to prevent Spark optimization)
 * */