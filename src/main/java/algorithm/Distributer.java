package algorithm;

import config.Properties;
import mapper.SimplePartionMapper;
import mapper.StringToTaxaTableMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import reducer.TaxaTableReducer;
import structure.TaxaTable;
import wqfm.utils.CombinationGenerator;

import java.util.*;

import static java.util.stream.Collectors.toList;
import static org.apache.spark.sql.functions.*;


public class Distributer {
    public static String runFunctions(String inputFileName, String outputFileName) {
        Dataset<Row> sortedQtDf = readFileInDf(inputFileName);
        // runCentalized(sortedQtDf);
        Dataset<Row> taggedDf = tagDataForPartition(sortedQtDf);
        Dataset<Row> treeDf = partitionAndRun(taggedDf);
        String final_tree_decoded = "tree";
        return final_tree_decoded;
    }


    public static String runCentalized(Dataset<Row> sortedWqDf) {
        sortedWqDf = sortedWqDf.withColumn("weightedQuartet", concat(col("value"), lit(" "), col("count")));
        List<String> qtList = sortedWqDf.select("weightedQuartet").as(Encoders.STRING()).collectAsList();
        String finalTree = new wQFMRunner().runDevideNConquer(qtList);
        return finalTree;
    }

    public static Dataset<Row> readFileInDf(String inputFileName) {
        Dataset<Row> sortedWqDf = config.Properties.SPARK.read().option("header", "true")
                .csv(Properties.HDFS_PATH + "/" + inputFileName)
                .orderBy(desc("count"));
        return sortedWqDf;
    }

    public static Dataset<Row> tagDataForPartition(Dataset<Row> sortedWqDf) {

        TaxaTable taxaTable = sortedWqDf
                .map(new StringToTaxaTableMapper(), Encoders.bean(TaxaTable.class))
                .reduce(new TaxaTableReducer());

        System.out.println("Final Taxa Table: " + taxaTable.toString());

        Map<String, ArrayList<String>> taxaPartitionMap = TaxaPartition.partitionTaxaList(taxaTable.TAXA_LIST);
        System.out.println(taxaPartitionMap);

        //t's because, List returned by subList() method is an instance of 'RandomAccessSubList' which is not serializable.
        // Therefore, you need to create a new ArrayList object from the list returned by the subList().
        UserDefinedFunction tagger = udf(
                (String qtStr) -> TaxaPartition.getTag(qtStr, taxaPartitionMap), DataTypes.StringType
        );
        Dataset<Row> taggedQtDf = sortedWqDf.withColumn("tag", tagger.apply(col("value")));
        taggedQtDf.groupBy(col("tag")).count().show();

        return taggedQtDf;
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


    public static Dataset<Row> partitionAndRun(Dataset<Row> taggedDf) {
        Dataset<Row> partitionedDf = taggedDf
                .withColumn("weightedQuartet", concat(col("value"), lit(" "), col("count")))
                // .filter(col("tag").notEqual("UNDEFINED"))
                .orderBy("tag"); // orderBy partitioned unique data to same partition
        // TaxaPartition.getPartitionDetail(partitionedDf);
        System.out.println("NumPartitions: " + partitionedDf.javaRDD().getNumPartitions());


        Dataset<Row> treeDf = partitionedDf.select("weightedQuartet")
                .mapPartitions((MapPartitionsFunction<Row, String>)  iterator -> {
                    ArrayList<String> arrayList = new ArrayList<>();
                    while (iterator.hasNext()) {
                        arrayList.add(iterator.next().getString(0));
                    }
                    String tree = "<NULL>";
                    if (arrayList.size() > 0) tree = new wQFMRunner().runDevideNConquer(arrayList);
                    return Collections.singletonList(tree).iterator();
                }, Encoders.STRING())
                .toDF();

        treeDf.filter(col("value").notEqual("<NULL>")).show(false);
        return treeDf;
    }
}
