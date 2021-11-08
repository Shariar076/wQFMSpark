package reducer;

import org.apache.spark.api.java.function.ReduceFunction;
import properties.ConfigValues;
import structure.TaxaTable;

import java.util.ArrayList;
import java.util.Collections;

public class TaxaTableReducer implements ReduceFunction<TaxaTable> {
    private static void updateTaxaTableWithTaxon(String taxon, TaxaTable taxaTable) {
        if (!taxaTable.TAXA_LIST.contains(taxon)) {
            taxaTable.TAXA_COUNT++;
            taxaTable.TAXA_LIST.add(taxon);
        }

    }
    // private static void updateTaxaTableWithTaxaList(TaxaTable taxaTable, TaxaTable t1) {
    //     for(String taxaListStr: t1.TAXA_GRP_COUNT.keySet()){
    //         if (!taxaTable.TAXA_GRP_COUNT.keySet().contains(taxaListStr)) {
    //             taxaTable.TAXA_GRP_COUNT.put(taxaListStr, 0);
    //         }
    //         taxaTable.TAXA_GRP_COUNT.put(taxaListStr, taxaTable.TAXA_GRP_COUNT.get(taxaListStr) + t1.TAXA_GRP_COUNT.get(taxaListStr));
    //     }
    // }

    private static int countTaxonInPartition(ArrayList<String> taxalist, ArrayList<String> partition) {
        int count = 0;
        for (String taxon : taxalist) {
            if (!partition.contains(taxon)) count++;
        }
        return count;
    }

    private static void updatePartition(ArrayList<String> taxalist, ArrayList<String> partition) {
        for (String taxon : taxalist) {
            if (!partition.contains(taxon)) partition.add(taxon);
        }
        Collections.sort(partition);
    }

    private static ArrayList<String> createPartition(ArrayList<String> taxalist) {
        Collections.sort(taxalist);
        return taxalist;
    }

    private static void updateTaxaTablePartitionsByTaxaListV1(ArrayList<String> taxaList, TaxaTable taxaTable) {
        ArrayList<ArrayList<String>> newPartitions = new ArrayList<>();
        if (taxaTable.TAXA_PARTITION_LIST.isEmpty()) newPartitions.add(createPartition(taxaList));
        else {
            //update
            for (ArrayList<String> partition : taxaTable.TAXA_PARTITION_LIST) {
                if (countTaxonInPartition(taxaList, partition) == taxaList.size()) {
                    //taxaList already contained by some partition
                    return;
                } else if (countTaxonInPartition(taxaList, partition) != 1) { // partition.size() < ConfigValues.TAXA_PER_PARTITION
                    updatePartition(taxaList, partition);
                    // no new partition needed
                    return;
                }
            }
            //insert
            for (ArrayList<String> partition : taxaTable.TAXA_PARTITION_LIST)
                if (countTaxonInPartition(taxaList, partition) == 1) { //single taxa overlap
                    newPartitions.add(createPartition(taxaList));
                    break;
                }
            // here means no update occurred
        }
        taxaTable.TAXA_PARTITION_LIST.addAll(newPartitions);
    }

    private static void updateTaxaTablePartitionsByTaxaListV2(ArrayList<String> taxaList, TaxaTable taxaTable) {
        ArrayList<ArrayList<String>> newPartitions = new ArrayList<>();
        if (taxaTable.TAXA_PARTITION_LIST.isEmpty()) newPartitions.add(createPartition(taxaList));
        else {
            //update
            for (ArrayList<String> partition : taxaTable.TAXA_PARTITION_LIST) {
                if (countTaxonInPartition(taxaList, partition) == taxaList.size()) {
                    //taxaList already contained by some partition
                    return;
                } else if (partition.size() + taxaList.size() < ConfigValues.TAXA_PER_PARTITION
                        && countTaxonInPartition(taxaList, partition) > 2) { // countTaxonInPartition(taxaList, partition) != 1
                    updatePartition(taxaList, partition);
                    // no new partition needed
                    return;
                }
            }
            //insert
            newPartitions.add(createPartition(taxaList));
        }
        taxaTable.TAXA_PARTITION_LIST.addAll(newPartitions);
    }

    private static void updateTaxaTablePartitionsByTaxaTablePartitionsV2(TaxaTable t1, TaxaTable taxaTable) {
        for (ArrayList<String> p1 : t1.TAXA_PARTITION_LIST) {
            boolean contained = false;
            for (ArrayList<String> partition : taxaTable.TAXA_PARTITION_LIST)
                if (partition.containsAll(p1)) {
                    contained = true;
                    break;
                }
            if (!contained) taxaTable.TAXA_PARTITION_LIST.add(p1);
        }
    }

    private static void updateTaxaTablePartitionsByTaxaListV3(ArrayList<String> taxaList, TaxaTable taxaTable) {
        taxaTable.TAXA_PARTITION_LIST.add(taxaList);
    }

    @Override
    public TaxaTable call(TaxaTable taxaTable, TaxaTable t1) throws Exception {
        for (String taxon : t1.TAXA_LIST) {
            updateTaxaTableWithTaxon(taxon, taxaTable);
        }
        // v1
        // updateTaxaTablePartitionsByTaxaListV1(t1.TAXA_LIST, taxaTable);
        // v2
        // if (t1.TAXA_PARTITION_LIST.isEmpty())
        //     updateTaxaTablePartitionsByTaxaListV2(t1.TAXA_LIST, taxaTable);  // t1: no reduce yet
        // else updateTaxaTablePartitionsByTaxaTablePartitionsV2(t1, taxaTable); // t1: already reduced
        // v3
        if (t1.TAXA_LIST.size() > 4) {
            System.out.println(t1.TAXA_LIST);
            updateTaxaTablePartitionsByTaxaListV3(taxaTable.TAXA_LIST, taxaTable);
        }
        return taxaTable;
    }
}
