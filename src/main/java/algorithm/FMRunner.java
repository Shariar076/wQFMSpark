package algorithm;

import config.Config;
import mapper.StringToQuartetMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import structure.InitialTable;
import structure.Quartet;

import java.util.ArrayList;
import java.util.Collections;

import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.col;


public class FMRunner {

    public static String runFunctions(String inputFileName, String outputFileName) {
        readFileAndPopulateInitialTable(inputFileName);

        // System.out.println("Reading from file <" + INPUT_FILE_NAME + "> done."
        //         + "\nInitial-Num-Quartets = " + initialTable.sizeTable());
        // System.out.println("Running with partition score " + WeightedPartitionScores.GET_PARTITION_SCORE_PRINT());
        int level = 0;
        // customDS.level = level; //for debugging issues.

        System.out.println(InitialTable.TAXA_COUNT);
        System.out.println(InitialTable.TAXA_LIST);
        System.out.println(InitialTable.map_of_str_vs_int_tax_list);
        System.out.println(InitialTable.map_of_int_vs_str_tax_list);
        // Sample sample = new Sample();
        // sample.testMapReduce();
        // can recursive func be static?
        String final_tree = recursiveDivideAndConquer(level); //customDS will have (P, Q, Q_relevant etc) all the params needed.

        // System.out.println("\n\n[L 49.] FMRunner: final tree return");
        //
        // System.out.println(final_tree);
        // String final_tree_decoded = IOHandler.getFinalTreeFromMap(final_tree, LegacyInitialTable.map_of_int_vs_str_tax_list);
        // System.out.println(final_tree_decoded);
        // IOHandler.writeToFile(final_tree_decoded, OUTPUT_FILE_NAME);

        String final_tree_decoded = "tree";
        return final_tree_decoded;
    }



    public static void readFileAndPopulateInitialTable(String inputFileName) {
        Dataset<Row> sortedWqDf = Config.SPARK.read().option("header", "true")
                .csv(Config.HDFS_PATH + "/" + inputFileName)
                .orderBy(desc("count"));

        // sortedWqDf.write().partitionBy("count").mode("overwrite").option("header", "true").csv("output/example.csv");
        InitialTable.quartetsTable = sortedWqDf.map(new StringToQuartetMapper(), Encoders.bean(Quartet.class));

        //this will enforce spark to perform the jobs accumulated so far
        //thus ensuring initialization of variables dependent on them
        System.out.println("Initial Table Size:" + InitialTable.quartetsTable.toDF().count());
    }

    public static String recursiveDivideAndConquer(int level) {
        String finalTree = "NONE";
        LegacyBipartition legBip = new LegacyBipartition();
        // legBip.runDevideNConquer(InitialTable.quartetsTable.collectAsList());
        JavaRDD<String> quartetJavaRDD =InitialTable.quartetsTable.toJavaRDD().mapPartitions(iterator -> {
            ArrayList<Quartet> arrayList = new ArrayList<>();
            while (iterator.hasNext()){
                arrayList.add(iterator.next());
            }
            String tree ="<TREE>";
            if(arrayList.size()>0) tree = legBip.runDevideNConquer(arrayList); // we can modify the customDs to accept iterator
            return Collections.singletonList(tree).iterator();
        });
        Dataset<Row> treeDf = Config.SPARK.createDataset(quartetJavaRDD.rdd(), Encoders.STRING()).toDF();
        treeDf.show();
        return finalTree;
    }


}
