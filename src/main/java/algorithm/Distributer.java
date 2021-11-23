package algorithm;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import phylonet.tree.io.ParseException;
import phylonet.tree.model.Tree;
import phylonet.tree.model.sti.STINode;
import phylonet.tree.model.sti.STITree;
import properties.ConfigValues;
import mapper.QuartetToTreeTablePartitionMapper;
import mapper.StringToTaxaTableMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import reducer.TaxaTableReducer;
import reducer.TreeReducer;
import structure.TaxaTable;
import structure.TreeTable;

import java.io.IOException;
import java.util.*;

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

    public static int getLeaveCountInTree(String treeStr) {
        STITree tree = null;
        try {
            tree = new STITree(treeStr);
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
        return tree.getLeafCount();
    }

    public static ArrayList<ArrayList<String>> getTaxaPartitions(String treeStr) throws Exception {
        ArrayList<ArrayList<String>> partitionList = new ArrayList<>();
        ArrayList<String> addedTaxa = new ArrayList<>();
        STITree tree = null;
        try {
            tree = new STITree(treeStr);
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < tree.getNodeCount(); i++) {
            STINode thisNode = tree.getNode(i);
            if (thisNode.getLeafCount() <= ConfigValues.TAXA_PER_PARTITION && thisNode.getLeafCount() >= 4) {
                boolean contained = false;
                List<String> leaves = TreeReducer.iteratorToList(thisNode.getLeaves().iterator());
                if (!addedTaxa.containsAll(leaves)) {
                    partitionList.add(new ArrayList<>(leaves));
                    addedTaxa.addAll(leaves);
                }
            }
            if (addedTaxa.size() == tree.getLeafCount()) break;
        }
        if (addedTaxa.size() != tree.getLeafCount()) {
            throw new Exception("Please increase Taxa per partition");
        }
        return partitionList;
    }

    public static ArrayList<ArrayList<String>> getTaxaPartitionsUnrooted(String treeStr) throws Exception {
        ArrayList<ArrayList<String>> partitionList = new ArrayList<>();
        ArrayList<String> addedTaxa = new ArrayList<>();
        STITree tree = null;
        List<Tree> rootedTrees = null;
        try {
            rootedTrees = new STITree(treeStr).getAllRootingTrees();
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
        Iterator<Tree> treeIterator = rootedTrees.iterator();
        boolean done = false;
        while (treeIterator.hasNext() && !done) {
            tree = (STITree) treeIterator.next();
            for (int i = 0; i < tree.getNodeCount(); i++) {
                STINode thisNode = tree.getNode(i);
                System.out.println(thisNode.getLeaves());
                if (thisNode.getLeafCount() <= ConfigValues.TAXA_PER_PARTITION && thisNode.getLeafCount() >= 4) {
                    List<String> leaves = TreeReducer.iteratorToList(thisNode.getLeaves().iterator());
                    if (!addedTaxa.containsAll(leaves)) {
                        partitionList.add(new ArrayList<>(leaves));
                        addedTaxa.addAll(leaves);
                    }
                }
                if (addedTaxa.size() == tree.getLeafCount()) {
                    done = true;
                    break;
                }
                System.out.println(partitionList);
            }
        }
        if (addedTaxa.size() != tree.getLeafCount()) {
            throw new Exception("Please increase Taxa per partition");
        }
        return partitionList;
    }

    public static String buildReferenceTree(Dataset<Row> sortedWqDf, int taxaCount) {
        System.out.println("================= Constructing Reference tree =================");
        List<Row> qtWeights = sortedWqDf.select("count").distinct().orderBy(desc("count")).collectAsList();
        System.out.println("Maximum iteration needed: " + qtWeights.size());
        Dataset<Row> refTreeDf = null;
        String refTree = "";
        int iterationCount = 0;
        for (Row weight : qtWeights) {
            iterationCount++;
            Dataset<Row> tempDf = sortedWqDf.filter(col("count")
                    .equalTo(weight.getAs("count")));
            if (refTreeDf == null) refTreeDf = tempDf;
            else refTreeDf = refTreeDf.union(tempDf);
            if (iterationCount % 2 == 0) {
                try {
                    refTree = runCentalized(refTreeDf);
                    if (getLeaveCountInTree(refTree) == taxaCount) break;
                } catch (Exception ignored) {
                }
            }
        }
        System.out.println("Final Reference Tree:" + refTree + "\nTotal iteration:" + iterationCount);
        return refTree;
    }

    public static ArrayList<String> chooseOverlappingQuartets(Dataset<Row> subsetWqDf, int taxaCount){
        ArrayList<String> addedTaxa= new ArrayList<>();
        ArrayList<String> quartetsList = new ArrayList<>();
        Dataset<Row> tempDf = subsetWqDf.withColumn("weightedQuartet", concat(col("value"), lit(" "), col("count")));
        for(Row qtRow: tempDf.collectAsList()){
            String qtString = qtRow.getAs("value");
            qtString = qtString.replace(" ", "");
            qtString = qtString.replace(";", ",");
            qtString = qtString.replace("(", "");
            qtString = qtString.replace(")", ""); // Finally end up with A,B,C,D,41.0
            ArrayList<String> taxaList = new ArrayList<>(Arrays.asList(qtString.split(",")).subList(0, 4));
            if(quartetsList.isEmpty()) {
                addedTaxa.addAll(taxaList);
                quartetsList.add(qtRow.getAs("weightedQuartet"));
            }
            else if(TaxaTable.countTaxonInPartition(taxaList, addedTaxa) > 0){
                TaxaTable.updatePartition(taxaList, addedTaxa);
                quartetsList.add(qtRow.getAs("weightedQuartet"));
            }
            if (addedTaxa.size()==taxaCount) break;
        }
        return quartetsList;
    }

    public static String buildReferenceTree2(Dataset<Row> sortedWqDf, int taxaCount) {
        System.out.println("================= Constructing Reference tree =================");
        List<Row> qtWeights = sortedWqDf.select("count").distinct().orderBy(desc("count")).collectAsList();
        System.out.println("Maximum iteration needed: " + qtWeights.size());
        ArrayList<String> chosenQuartets = new ArrayList<>();
        String refTree = "";
        int iterationCount = 0;
        for (Row weight : qtWeights) {
            iterationCount++;
            Dataset<Row> tempDf = sortedWqDf.filter(col("count").equalTo(weight.getAs("count")));
            chosenQuartets.addAll(chooseOverlappingQuartets(tempDf, taxaCount));
            try {
                refTree = new wQFMRunner().runDevideNConquer(chosenQuartets, "*");
                if (getLeaveCountInTree(refTree) == taxaCount) break;
            } catch (Exception ignored) {
            }
        }
        System.out.println("Final Reference Tree:" + refTree + "\nTotal iteration:" + iterationCount);
        return refTree;
    }

    public static String readReferenceTree() {
        System.out.println("================= Reading Reference tree =================");
        Dataset<Row> df = ConfigValues.SPARK.read().text(ConfigValues.HDFS_PATH + ConfigValues.REFERENCE_FILE_NAME);
        String rawTree = df.first().getAs("value");
        String refTree = rawTree.split(";")[0] + ";";
        System.out.println("Final Reference Tree:" + refTree);
        return refTree;
    }

    public static Dataset<Row> tagQuartetByGroup(Dataset<Row> initialDf, Map<String, ArrayList<String>> taxaPartitionMap) {
        UserDefinedFunction tagger = udf(
                (String qtStr) -> TaxaPartition.getTag(qtStr, taxaPartitionMap), DataTypes.StringType
        );
        Dataset<Row> taggedQtDf = initialDf.withColumn("tag", tagger.apply(col("value")));

        taggedQtDf.groupBy(col("tag")).count().show(false);
        return taggedQtDf;
    }

    public static String createInterPartitionTree(Dataset<Row> taggedQtDf, Map<String, ArrayList<String>> taxaPartitionMap) {
        String interPartitionTree = null;
        if (taxaPartitionMap.size() > 3) { // at least 4 partition is needed for constructing quartets
            UserDefinedFunction isQuartet = udf(
                    (UDF1<String, Object>) TaxaPartition::isValidQuartet, DataTypes.BooleanType
            );
            Dataset<Row> updatedDf = taggedQtDf.filter(col("tag").equalTo("UNDEFINED"))
                    .map((MapFunction<Row, String>) r -> TaxaPartition.updateUndefined(r.getAs("value"), taxaPartitionMap), Encoders.STRING())
                    .toDF()
                    .where(isQuartet.apply(col("value")))
                    .groupBy("value").count();

            System.out.println("Valid quartets in UNDEFINED: " + updatedDf.count());
            updatedDf.show(false);
            interPartitionTree = runCentalized(updatedDf);
        } else {
            if (taxaPartitionMap.size() == 2) interPartitionTree = "(t_0,t_1);";
            else if (taxaPartitionMap.size() == 3) {
                UserDefinedFunction isTriplet = udf(
                        (UDF1<String, Object>) TaxaPartition::isValidTriptet, DataTypes.BooleanType
                );
                Row maxWeightRow = taggedQtDf.filter(col("tag").equalTo("UNDEFINED"))
                        .map((MapFunction<Row, String>) r -> TaxaPartition.updateUndefined(r.getAs("value"), taxaPartitionMap), Encoders.STRING())
                        .toDF()
                        .where(isTriplet.apply(col("value")))
                        .groupBy("value").count()
                        .orderBy(desc("count")).limit(1).first();
                interPartitionTree = TaxaPartition.getTripletStr(maxWeightRow.getAs("value"));
            }

        }
        System.out.println("InterPartitionTree: " + interPartitionTree);
        return interPartitionTree;
    }


    public static String partitionDataAndRun(Dataset<Row> sortedWqDf, TaxaTable taxaTable) {
        long time_1 = System.currentTimeMillis();
        String refTree = readReferenceTree(); //buildReferenceTree2(sortedWqDf, taxaTable.TAXA_COUNT);
        System.out.println("Reference Tree Construction Complete, Elapsed time: " + (System.currentTimeMillis() - time_1 + " ms"));
        try {
            taxaTable.TAXA_PARTITION_LIST = getTaxaPartitions(refTree);
        } catch (Exception e) {
            e.printStackTrace();
        }
        ///////////////////////////////////////// groupTaxaAndTagData//////////////////////////////////
        // Map<String, ArrayList<String>> taxaPartitionMap = TaxaPartition.partitionTaxaListByCombination(taxaTable.TAXA_LIST);
        Map<String, ArrayList<String>> taxaPartitionMap = TaxaPartition.partitionTaxaListByTaxaTable(taxaTable.TAXA_PARTITION_LIST);
        // Map<String, ArrayList<String>> taxaPartitionMap = TaxaPartition.partitionInDisjointGroups(taxaTable.TAXA_LIST);
        //Print partitionMap
        for (Map.Entry<String, ArrayList<String>> partition : taxaPartitionMap.entrySet()) {
            System.out.println(partition);
        }

        Dataset<Row> taggedQtDf = tagQuartetByGroup(sortedWqDf, taxaPartitionMap);
        //////////////////////////////////////////////////////////////////////////////////////////////////

        String interPartitionTree = createInterPartitionTree(taggedQtDf, taxaPartitionMap);

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
        time_1 = System.currentTimeMillis();
        String finalTree = runPartitionsAndGetTree(partitionedDf, taxaTable, interPartitionTree);
        ConfigValues.SPARK.stop();

        System.out.println("All partitioned Tasks Complete, Elapsed time: " + (System.currentTimeMillis() - time_1 + " ms"));

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