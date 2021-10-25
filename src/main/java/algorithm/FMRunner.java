package algorithm;

import config.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import structure.InitialTableSpark;

import java.util.List;

import static org.apache.spark.sql.functions.*;


public class FMRunner {
    public static String runFunctions(String inputFileName, String outputFileName) {
        readFileAndPopulateInitialTable(inputFileName);

        // System.out.println("Reading from file <" + INPUT_FILE_NAME + "> done."
        //         + "\nInitial-Num-Quartets = " + initialTable.sizeTable());
        // System.out.println("Running with partition score " + WeightedPartitionScores.GET_PARTITION_SCORE_PRINT());
        int level = 0;
        // customDS.level = level; //for debugging issues.

        System.out.println(InitialTableSpark.TAXA_COUNT);
        System.out.println(InitialTableSpark.TAXA_LIST);
        System.out.println(InitialTableSpark.map_of_str_vs_int_tax_list);
        System.out.println(InitialTableSpark.map_of_int_vs_str_tax_list);
        // Sample sample = new Sample();
        // sample.testMapReduce();
        // can recursive func be static?
        // String final_tree = recursiveDivideAndConquer(level); //customDS will have (P, Q, Q_relevant etc) all the params needed.

        // System.out.println("\n\n[L 49.] FMRunner: final tree return");
        //
        // System.out.println(final_tree);
        // String final_tree_decoded = IOHandler.getFinalTreeFromMap(final_tree, InitialTable.map_of_int_vs_str_tax_list);
        // System.out.println(final_tree_decoded);
        // IOHandler.writeToFile(final_tree_decoded, OUTPUT_FILE_NAME);

        String final_tree_decoded = "tree";
        return final_tree_decoded;
    }



    public static void readFileAndPopulateInitialTable(String inputFileName) {
        Dataset<Row> sortedWqDf = Properties.SPARK.read().option("header", "true")
                .csv(Properties.HDFS_PATH + "/" + inputFileName)
                .orderBy(desc("count"));

        sortedWqDf = sortedWqDf.withColumn("weightedQuartet", concat(col("value"), lit(" "),col("count")));
        List<String> qtList = sortedWqDf.select("weightedQuartet").as(Encoders.STRING()).collectAsList();
        System.out.println(new wQFMRunner().runDevideNConquer(qtList));
        // sortedWqDf.write().partitionBy("count").mode("overwrite").option("header", "true").csv("output/example.csv");
        // InitialTableSpark.quartetsTable = sortedWqDf.map(new StringToQuartetMapper(), Encoders.bean(SerialQuartet.class));

        //this will enforce spark to perform the jobs accumulated so far
        //thus ensuring initialization of variables dependent on them
        // System.out.println("Initial Table Size:" + InitialTableSpark.quartetsTable.toDF().count());
    }

    // public static String recursiveDivideAndConquer(int level) {
    //     String finalTree = "NONE";
    //     wQFMRunner legBip = new wQFMRunner();
    //     // legBip.runDevideNConquer(InitialTableSpark.quartetsTable.collectAsList());
    //     JavaRDD<String> treeJavaRDD =InitialTableSpark.quartetsTable.toJavaRDD().mapPartitions(iterator -> {
    //         ArrayList<SerialQuartet> arrayList = new ArrayList<>();
    //         // if(iterator.hasNext())arrayList.add(iterator.next());
    //         while (iterator.hasNext()){
    //             arrayList.add(iterator.next());
    //         }
    //         String tree ="<TREE>";
    //         // if(arrayList.size()>0) tree = legBip.runDevideNConquer(arrayList);
    //         return Collections.singletonList(tree).iterator();
    //     });
    //     Dataset<Row> treeDf = Properties.SPARK.createDataset(treeJavaRDD.rdd(), Encoders.STRING()).toDF();
    //     treeDf.show();
    //     return finalTree;
    // }


}
