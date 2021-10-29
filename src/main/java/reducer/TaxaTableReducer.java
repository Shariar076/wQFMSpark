package reducer;

import org.apache.spark.api.java.function.ReduceFunction;
import structure.TaxaTable;

import java.util.ArrayList;
import java.util.Arrays;
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

    private static boolean allTaxonInPartition(ArrayList<String> taxalist, ArrayList<String> partition) {
        for (String taxon : taxalist) {
            if (!partition.contains(taxon)) return false;
        }
        return true;
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

    private static void updateTaxaTablePartitions(ArrayList<String> taxaList, TaxaTable taxaTable) {
        ArrayList<ArrayList<String>> newPartitions = new ArrayList<>();
        for (ArrayList<String> partition : taxaTable.TAXA_PARTITION_LIST) {
            if(allTaxonInPartition(taxaList, partition)){
                //taxaList already contained by some partition
                return;
            }
            if (partition.size() < 8) { // either  8 or 12 or ...
                updatePartition(taxaList, partition);
                // no new partition needed
                return;
            }
        }
        // here means no update occurred
        newPartitions.add(createPartition(taxaList));
        taxaTable.TAXA_PARTITION_LIST.addAll(newPartitions);
    }

    @Override
    public TaxaTable call(TaxaTable taxaTable, TaxaTable t1) throws Exception {
        for (String taxon : t1.TAXA_LIST) {
            updateTaxaTableWithTaxon(taxon, taxaTable);
        }
        // updateTaxaTableWithTaxaList(taxaTable, t1);
        updateTaxaTablePartitions(t1.TAXA_LIST, taxaTable);
        return taxaTable;
    }
}
