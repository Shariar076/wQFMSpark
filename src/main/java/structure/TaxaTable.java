package structure;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.sql.Dataset;
import properties.ConfigValues;

import java.io.Serializable;
import java.util.*;

public class TaxaTable implements Serializable {
    public int TAXA_COUNT = 0;
    public ArrayList<String> TAXA_LIST = new ArrayList<>();

    // public Map<String,Integer> TAXA_GRP_COUNT = new HashMap<>();

    public ArrayList<ArrayList<String>> TAXA_PARTITION_LIST =  new ArrayList<>();

    public TaxaTable() {
    }

    public int getTAXA_COUNT() {
        return TAXA_COUNT;
    }

    public void setTAXA_COUNT(int TAXA_COUNT) {
        this.TAXA_COUNT = TAXA_COUNT;
    }

    public ArrayList<String> getTAXA_LIST() {
        return TAXA_LIST;
    }

    public void setTAXA_LIST(ArrayList<String> TAXA_LIST) {
        this.TAXA_LIST = TAXA_LIST;
    }

    public ArrayList<ArrayList<String>> getTAXA_PARTITION_LIST() {
        return TAXA_PARTITION_LIST;
    }

    public void setTAXA_PARTITION_LIST(ArrayList<ArrayList<String>> TAXA_PARTITION_LIST) {
        this.TAXA_PARTITION_LIST = TAXA_PARTITION_LIST;
    }

    @Override
    public String toString() {
        return "TaxaTable{" +
                "TAXA_COUNT=" + TAXA_COUNT +
                ",\n TAXA_LIST=" + TAXA_LIST +
                ",\n TAXA_PARTITION_LIST Size=" + TAXA_PARTITION_LIST.size()+
                '}';
    }

    public void updateTaxaTableWithTaxon(String taxon) {
        if (!this.TAXA_LIST.contains(taxon)) {
            this.TAXA_COUNT++;
            this.TAXA_LIST.add(taxon);
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
            if (partition.contains(taxon)) count++;
        }
        return count;
    }

    private static void updatePartition(ArrayList<String> taxalist, ArrayList<String> partition) {
        for (String taxon : taxalist) {
            if (!partition.contains(taxon)) partition.add(taxon);
        }
        // Collections.sort(partition);
    }

    private static ArrayList<String> createPartition(ArrayList<String> taxalist) {
        Collections.sort(taxalist);
        return taxalist;
    }

    public void updateTaxaTablePartitionsByTaxaListV1(ArrayList<String> taxaList) {
        ArrayList<ArrayList<String>> newPartitions = new ArrayList<>();
        if (this.TAXA_PARTITION_LIST.isEmpty()) newPartitions.add(createPartition(taxaList));
        else {
            //update
            for (ArrayList<String> partition : this.TAXA_PARTITION_LIST) {
                if (partition.containsAll(taxaList)) {
                    //taxaList already contained by some partition
                    return;
                } else if (countTaxonInPartition(taxaList, partition) != 1) { // partition.size() < ConfigValues.TAXA_PER_PARTITION
                    updatePartition(taxaList, partition);
                    // no new partition needed
                    return;
                }
            }
            //insert
            for (ArrayList<String> partition : this.TAXA_PARTITION_LIST)
                if (countTaxonInPartition(taxaList, partition) == 1) { //single taxa overlap
                    newPartitions.add(createPartition(taxaList));
                    break;
                }
            // here means no update occurred
        }
        this.TAXA_PARTITION_LIST.addAll(newPartitions);
    }

    public void updateTaxaTablePartitionsByTaxaListV2(ArrayList<String> taxaList) {
        ArrayList<ArrayList<String>> newPartitions = new ArrayList<>();
        if (this.TAXA_PARTITION_LIST.isEmpty()) newPartitions.add(createPartition(taxaList));
        else {
            //update
            for (ArrayList<String> partition : this.TAXA_PARTITION_LIST) {
                //taxaList already contained by some partition
                if (partition.containsAll(taxaList)) return;
            }
            for (ArrayList<String> partition : this.TAXA_PARTITION_LIST) {
                // no new partition needed if space is available
                if (partition.size() + taxaList.size() < ConfigValues.TAXA_PER_PARTITION){ //countTaxonInPartition(taxaList, partition) > 2
                    updatePartition(taxaList, partition);
                    return;
                }
            }
            //insert
            for (ArrayList<String> partition : this.TAXA_PARTITION_LIST) {
                if (countTaxonInPartition(taxaList, partition) > 0){ // over-lap
                    newPartitions.add(createPartition(taxaList));
                    break;
                }
            }
            // newPartitions.add(createPartition(taxaList));
        }
        this.TAXA_PARTITION_LIST.addAll(0, newPartitions);
    }

    public void updateTaxaTablePartitionsByTaxaTablePartitionsV2(TaxaTable t1) {
        for (ArrayList<String> p1 : t1.TAXA_PARTITION_LIST) {
            boolean contained = false;
            for (int i =0; i<this.TAXA_PARTITION_LIST.size(); i++){
                ArrayList<String> partition = this.TAXA_PARTITION_LIST.get(i);
                if (partition.containsAll(p1)) {
                    contained = true;
                    break;
                }
                else if(p1.containsAll(partition)) {
                    this.TAXA_PARTITION_LIST.set(i, p1);
                    break;
                }
            }
            if (!contained) this.TAXA_PARTITION_LIST.add(p1);
        }
    }

    public  void updateTaxaTablePartitionsByTaxaListV3(ArrayList<String> taxaList) {
        this.TAXA_PARTITION_LIST.add(taxaList);
    }
}
