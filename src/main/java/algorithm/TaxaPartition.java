package algorithm;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import structure.TaxaTable;
import util.CombinationGenerator;

import java.util.*;

import static java.util.stream.Collectors.toList;


public class TaxaPartition {
    public static Map<String, ArrayList<String>> partitionTaxaListByTaxaTable(ArrayList<ArrayList<String>> taxaPartitionList) {
        Map<String, ArrayList<String>> mapPartition = new HashMap<>();

        for (int i=0; i<taxaPartitionList.size();i++) {
            ArrayList<String> list = taxaPartitionList.get(i);
            String key = String.valueOf(i);
            mapPartition.put(key, list);
        }
        return mapPartition;
    }

    public static Map<String, ArrayList<String>> partitionTaxaListByCombination(ArrayList<String> taxaList) {
        Map<String, ArrayList<String>> mapPartition = new HashMap<>();

        CombinationGenerator combinationGenerator = new CombinationGenerator();
        List<int[]> combinations = combinationGenerator.generate(taxaList.size(), taxaList.size() - 1);

        for (int[] combination : combinations) {
            List<String> taxaPartition = Arrays.stream(combination).mapToObj(taxaList::get).collect(toList());
            ArrayList<String> list = new ArrayList<>(taxaPartition);
            String key = String.join("-", list);
            mapPartition.put(key, list);
        }
        return mapPartition;
    }

    public static Map<String, ArrayList<String>> partitionTaxaListByWindow(ArrayList<String> taxaList) {
        Map<String, ArrayList<String>> mapPartition = new HashMap<>();
        int windowSize = 8; //taxaList.size()-1;
        int startIdx = 0;
        int slide = 1;
        // remember fromIdx low endpoint (inclusive) of the subList and toIdx is high endpoint (exclusive) of the subList
        while (startIdx + windowSize <= taxaList.size()) {
            ArrayList<String> list = new ArrayList<>(taxaList.subList(startIdx, startIdx + windowSize));
            startIdx += slide;
            String key = String.join("-", list);
            mapPartition.put(key, list);
            // if (startIdx<taxaList.size()&&startIdx+windowSize>taxaList.size()){
            //     ArrayList<String> listRest = new ArrayList<>(taxaList.subList(startIdx, taxaList.size()));
            //     String keyRest = String.join("-", listRest);
            //     mapPartition.put(keyRest, listRest);
            // }
        }
        return mapPartition;
    }

    public static boolean quartetInPartition(List<String> partition, List<String> qtTaxaList) {
        for (String taxon : qtTaxaList) {
            if (!partition.contains(taxon)) return false;
        }
        return true;
    }

    public static String getTag(String qtString, Map<String, ArrayList<String>> taxaPartitionMap) {
        qtString = qtString.replace(" ", "");
        qtString = qtString.replace(";", ",");
        qtString = qtString.replace("(", "");
        qtString = qtString.replace(")", ""); // Finally end up with A,B,C,D,41.0
        List<String> taxaSubList = new ArrayList<>(Arrays.asList(qtString.split(","))).subList(0, 4);
        ArrayList<String> taxaList = new ArrayList<>(taxaSubList);

        for (String key : taxaPartitionMap.keySet()) {
            if (quartetInPartition(taxaPartitionMap.get(key), taxaList)) return key;
        }
        return "UNDEFINED";
    }

    public static Iterator<Integer> countDataOnPArtition(Iterator<Row> iterator) {
        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        return Collections.singletonList(count).iterator();
    }

    public static void getPartitionDetail(Dataset<Row> partitionedDf) {
        System.out.println("Printing Partition details");
        // partitionedDf.explain(true);
        // Dataset<Integer> rowCountDs = partitionedDf.mapPartitions(new SimplePartionMapper(), Encoders.INT());
        Dataset<Integer> rowCountDs = partitionedDf.mapPartitions((MapPartitionsFunction<Row, Integer>)
                TaxaPartition::countDataOnPArtition, Encoders.INT());
        rowCountDs.show((int) rowCountDs.count());
        System.out.println("NumPartitions: " + partitionedDf.javaRDD().getNumPartitions());
    }
}
