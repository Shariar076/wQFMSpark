package wqfm.ds;

import java.util.*;

public class CustomDSPerLevel {
    public InitialTable initial_table1_of_list_of_quartets; //immutable [doesn't change, only as reference, is passed]

    public int level;

    //Will mutate per level
    public List<Integer> quartet_indices_list_unsorted;
    public Map<Integer, List<Integer>> map_taxa_relevant_quartet_indices; //releveant quartets map, key: taxa & val:list<indices>
    public Map<Double, List<Integer>> sorted_quartets_weight_list_indices_map;

    public List<Integer> taxa_list_int;

    public void setInitialTableReference(InitialTable initTable) {
        this.initial_table1_of_list_of_quartets = initTable;
    }

    public CustomDSPerLevel() {
        this.quartet_indices_list_unsorted = new ArrayList<>();
        this.map_taxa_relevant_quartet_indices = new HashMap<>();
        this.sorted_quartets_weight_list_indices_map = new TreeMap<>(Collections.reverseOrder());
        this.taxa_list_int = new ArrayList<>();
    }

    public void sortQuartetIndicesMap() {
        for (int i = 0; i < this.quartet_indices_list_unsorted.size(); i++) {
            int qrt_index = this.quartet_indices_list_unsorted.get(i);
            Quartet q = this.initial_table1_of_list_of_quartets.get(qrt_index);
            if (this.sorted_quartets_weight_list_indices_map.containsKey(q.weight) == false) { //initialize the list [this weight doesn't exist]
                this.sorted_quartets_weight_list_indices_map.put(q.weight, new ArrayList<>());
            }
            this.sorted_quartets_weight_list_indices_map.get(q.weight).add(qrt_index); //append to list in treeMap
        }
    }

    public void fillRelevantQuartetsMap() {
        //For each quartet
        for (int itr = 0; itr < this.quartet_indices_list_unsorted.size(); itr++) {
            int index_qrt = this.quartet_indices_list_unsorted.get(itr);
            Quartet q = this.initial_table1_of_list_of_quartets.get(index_qrt);
            for (int i = 0; i < Quartet.NUM_TAXA_PER_PARTITION; i++) { // Do for left-sisters ... push to map THIS quartet's row,col
                int taxon = q.taxa_sisters_left[i];
                if (this.map_taxa_relevant_quartet_indices.containsKey(taxon) == false) { //map doesn't have an entry yet for THIS taxon
                    this.map_taxa_relevant_quartet_indices.put(taxon, new ArrayList<>()); // initialize for THIS taxon
                }
                this.map_taxa_relevant_quartet_indices.get(taxon).add(index_qrt);
            }
            for (int i = 0; i < Quartet.NUM_TAXA_PER_PARTITION; i++) { // Repeat the same for right-sisters
                int taxon = q.taxa_sisters_right[i];
                if (this.map_taxa_relevant_quartet_indices.containsKey(taxon) == false) { //map doesn't have an entry yet for THIS taxon
                    this.map_taxa_relevant_quartet_indices.put(taxon, new ArrayList<>()); // initialize for THIS taxon
                }
                this.map_taxa_relevant_quartet_indices.get(taxon).add(index_qrt);
            }
        }
    }

    public void fillUpTaxaList() {
        this.map_taxa_relevant_quartet_indices.keySet().forEach((tax) -> {
            this.taxa_list_int.add(tax);
        });
    }

}
