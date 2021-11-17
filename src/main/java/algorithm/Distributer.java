package algorithm;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import properties.ConfigValues;
import mapper.QuartetToTreeTablePartitionMapper;
import mapper.StringToTaxaTableMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import properties.DefaultConfigs;
import reducer.TaxaTableReducer;
import reducer.TreeReducer;
import structure.TaxaTable;
import structure.TreeTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;


public class Distributer {
    public static String runFunctions(String inputFileName, String outputFileName) {
        Dataset<Row> sortedQtDf = readFileInDf(inputFileName);
        TaxaTable taxaTable = initialiZeTaxaTable(sortedQtDf);
        // Dataset<Row> taggedDf = groupTaxaAndTagData(sortedQtDf, taxaTable);
        String distributedRunTree = partitionDataAndRun(sortedQtDf, taxaTable);
        // String centralizedRunTree = runCentalized(sortedQtDf);
        System.out.println("distributedRunTree: " + distributedRunTree);
        // System.out.println("centralizedRunTree: " + centralizedRunTree);
        return distributedRunTree;
    }


    public static String runCentalized(Dataset<Row> sortedWqDf) {
        sortedWqDf = sortedWqDf.withColumn("weightedQuartet", concat(col("value"), lit(" "), col("count")));
        List<String> qtList = sortedWqDf.select("weightedQuartet").as(Encoders.STRING()).collectAsList();
        return new wQFMRunner().runDevideNConquer(qtList, "*");
    }

    public static Dataset<Row> readFileInDf(String inputFileName) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("value", DataTypes.StringType, false),
                DataTypes.createStructField("count", DataTypes.DoubleType, false)
        });

        return ConfigValues.SPARK.read()
                .option("sep", " ")
                .schema(schema)
                .csv(ConfigValues.HDFS_PATH + "/" + inputFileName);
        // .orderBy(desc("count"));
    }

    public static TaxaTable initialiZeTaxaTable(Dataset<Row> sortedWqDf) {
        TaxaTable taxaTable = sortedWqDf
                .map(new StringToTaxaTableMapper(), Encoders.bean(TaxaTable.class))
                .reduce(new TaxaTableReducer());

        System.out.println("Final Taxa Table: " + taxaTable.toString());
        return taxaTable;
    }

    private static String runExplained(Dataset<Row> partitionedDf, TaxaTable taxaTable) {
        Dataset<Row> treeTableDf = partitionedDf.select("weightedQuartet", "tag", "count")
                .mapPartitions(new QuartetToTreeTablePartitionMapper(), Encoders.bean(TreeTable.class))
                .persist() // .cache() //avoid lazy execution i.e. running twice
                .orderBy(desc("support"))
                .filter(col("tree").notEqual("<NULL>"))
                .toDF();

        System.out.println("Total generated trees: " + treeTableDf.count());
        treeTableDf.show(false);
        // TreeReducer treeReducer = new TreeReducer(taxaTable.TAXA_LIST);

        return treeTableDf
                .map((MapFunction<Row, String>) r -> r.getAs("tree"), Encoders.STRING())
                // .collectAsList()
                // .stream().reduce(null, treeReducer::call);
                .reduce(new TreeReducer(taxaTable.TAXA_LIST));
    }

    private String runSilent(Dataset<Row> partitionedDf, TaxaTable taxaTable) {
        return partitionedDf.select("weightedQuartet", "tag", "count")
                .mapPartitions(new QuartetToTreeTablePartitionMapper(), Encoders.bean(TreeTable.class))
                .persist() //.cache() //avoid lazy execution i.e. running twice
                .orderBy(desc("support"))
                .filter(col("tree").notEqual("<NULL>"))
                .toDF()
                .select("tree")
                .map((MapFunction<Row, String>) r -> r.getAs("tree"), Encoders.STRING())
                .reduce(new TreeReducer(taxaTable.TAXA_LIST));
    }

    private static String runPartitionsAndGetTree(Dataset<Row> partitionedDf, TaxaTable taxaTable, String interPartitionTree) {
        Dataset<Row> treeTableDf = partitionedDf.select("weightedQuartet", "tag", "count")
                .mapPartitions(new QuartetToTreeTablePartitionMapper(), Encoders.bean(TreeTable.class))
                .persist() // .cache() //avoid lazy execution i.e. running twice
                .orderBy(desc("support"))
                .filter(col("tree").notEqual("<NULL>"))
                .toDF();

        System.out.println("Total generated trees: " + treeTableDf.count());
        treeTableDf.show(false);
        // TreeReducer treeReducer = new TreeReducer(taxaTable.TAXA_LIST);

        for (Row row : treeTableDf.collectAsList()) {
            String treeNode = "t_" + row.getAs("tag");
            String nodeTree = row.getAs("tree");
            interPartitionTree = interPartitionTree.replace(treeNode, nodeTree.substring(0, nodeTree.length() - 1));
        }
        return interPartitionTree;
    }

    public static String partitionDataAndRun(Dataset<Row> sortedWqDf, TaxaTable taxaTable) {
        ///////////////////////////////////////// groupTaxaAndTagData//////////////////////////////////
        // Map<String, ArrayList<String>> taxaPartitionMap = TaxaPartition.partitionTaxaListByCombination(taxaTable.TAXA_LIST);
        // Map<String, ArrayList<String>> taxaPartitionMap = TaxaPartition.partitionTaxaListByTaxaTable(taxaTable.TAXA_PARTITION_LIST);

        Map<String, ArrayList<String>> taxaPartitionMap = null;
        Dataset<Row> taggedQtDf = null;
        long minUndefined = sortedWqDf.count();

        for (int i = 0; i < 10; i++) { //check multiple partitions
            Map<String, ArrayList<String>> tempPartitionMap = TaxaPartition.partitionInDisjointGroups(taxaTable.TAXA_LIST);
            //it's because, List returned by subList() method is an instance of 'RandomAccessSubList' which is not serializable.
            // Therefore, you need to create a new ArrayList object from the list returned by the subList().
            UserDefinedFunction tagger = udf(
                    (String qtStr) -> TaxaPartition.getTag(qtStr, tempPartitionMap), DataTypes.StringType
            );
            Dataset<Row> tempTaggedDf = sortedWqDf.withColumn("tag", tagger.apply(col("value")));
            if (tempTaggedDf.filter(col("tag").equalTo("UNDEFINED")).count() < minUndefined) {
                minUndefined = tempTaggedDf.filter(col("tag").equalTo("UNDEFINED")).count();
                System.out.println("UNDEFINED count: "+minUndefined);
                taxaPartitionMap = tempPartitionMap;
                taggedQtDf = tempTaggedDf;
            }
            Collections.shuffle(taxaTable.TAXA_LIST);
        }

        //Print partitionMap
        for (Map.Entry<String, ArrayList<String>> partition : taxaPartitionMap.entrySet()) {
            System.out.println(partition);
        }


        taggedQtDf.groupBy(col("tag")).count().show(false);

        UserDefinedFunction isQuartet = udf(
                (String qtStr) -> TaxaPartition.isValidQuartet(qtStr), DataTypes.BooleanType
        );
        Map<String, ArrayList<String>> finalTaxaPartitionMap = taxaPartitionMap;
        Dataset<Row> updatedDf = taggedQtDf.filter(col("tag").equalTo("UNDEFINED"))
                .map((MapFunction<Row, String>) r -> TaxaPartition.updateUndefined(r.getAs("value"), finalTaxaPartitionMap),
                        Encoders.STRING())
                .toDF()
                .where(isQuartet.apply(col("value")))
                .groupBy("value").count();

        System.out.println("Valid quartets in UNDEFINED: " + updatedDf.count());
        updatedDf.show(false);
        String undefinedDfTree = runCentalized(updatedDf);
        System.out.println("InterPartitionTree: " + undefinedDfTree);

        //////////////////////////////////////////////////////////////////////////////////////////////////

        int numPartitions = (int) taggedQtDf.groupBy(col("tag")).count().count();
        System.out.println("Number of Partitions by taxaPartition: " + numPartitions);
        Dataset<Row> partitionedDf = taggedQtDf
                .withColumn("weightedQuartet", concat(col("value"), lit(" "), col("count")))
                .filter(col("tag").notEqual("UNDEFINED"))
                .repartitionByRange(col("tag")); // orderBy partitioned unique data to same partition

        // TaxaPartition.getPartitionDetail(partitionedDf);

        // partitionedDf.select("weightedQuartet", "tag").write().partitionBy("tag")
        //         .mode("overwrite").option("header", "false")
        //         .csv(ConfigValues.HDFS_PATH + "/" + DefaultConfigs.INPUT_FILE_NAME_WQRTS_PARTITIONED);
        // // IOHandler.runSystemCommand(Config.PYTHON_ENGINE+ " ./scripts/test.py --input input/partitioned-weighted-quartets.csv --tag A-E-F-H-M-O");
        System.out.println("Number of Data Partitions: " + partitionedDf.javaRDD().getNumPartitions());

        System.out.println("Partitioning Tasks to workers ...");
        long time_1 = System.currentTimeMillis();
        String finalTree = runPartitionsAndGetTree(partitionedDf, taxaTable, undefinedDfTree);
        ConfigValues.SPARK.stop();

        System.out.println("All partitioned Tasks Complete, Elapsed time: " + (System.currentTimeMillis() - time_1));

        // treeDs.show(false);
        System.out.println("Final tree " + finalTree);

        return finalTree;
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
 *
 * Repartitioning is based on hash partitioning (take the hash code of the partitioning key modulo the number of partitions),
 * so whether each partition only has one value is purely chance.
 * If you can map each partitioning key to a unique Int in the range of zero to (number of unique values - 1),
 * since the hash code of an Int in Scala is that integer, this would ensure that if there are at least as many partitions
 * as there are unique values, no partition has multiple distinct partitioning key values.
 * That said, coming up with the assignment of values to such Ints is inherently not parallelizable
 * and requires either a sequential scan or knowing the distinct values ahead of time.
 * Probabilistically, the chance that a particular value hashes into a particular partition of (n partitions) is 1/n.
 * As n increases relative to the number of distinct values, the chance of no partition having more than one distinct
 * value increases (at the limit, if you could have 2^32 partitions, nearly all of them would be empty but an actual
 * hash collision would still guarantee multiple distinct values in a partition). So if you can tolerate empty partitions,
 *
 * choosing a number of partitions that's sufficiently greater than the number of distinct values would reduce the chance of a sub-ideal result.
 * */