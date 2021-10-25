package bipartition;

import config.DefaultValues;
import org.apache.spark.sql.Row;
import structure.Quartet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class InitialBipartition implements Serializable {
    private Map<Integer, Integer> partitionMap;
    // private final Dataset<Quartet> quartetDataset;
    private int count_taxa_left_partition;
    private int count_taxa_right_partition;

    public InitialBipartition(ArrayList<Integer> taxaList){
        this.partitionMap = new HashMap<>();
        this.count_taxa_left_partition = 0;
        this.count_taxa_right_partition = 0;
        for (int tax : taxaList) { //initially assign all as 0/unassigned
            partitionMap.put(tax, DefaultValues.UNASSIGNED_PARTITION);
        }
        // System.out.println(partitionMap);
    }

    public Map<Integer, Integer> getPartitionMap() {
        return partitionMap;
    }

    public void setPartitionMap(Map<Integer, Integer> partitionMap) {
        this.partitionMap = partitionMap;
    }

    public int getCount_taxa_left_partition() {
        return count_taxa_left_partition;
    }

    public void setCount_taxa_left_partition(int count_taxa_left_partition) {
        this.count_taxa_left_partition = count_taxa_left_partition;
    }

    public int getCount_taxa_right_partition() {
        return count_taxa_right_partition;
    }

    public void setCount_taxa_right_partition(int count_taxa_right_partition) {
        this.count_taxa_right_partition = count_taxa_right_partition;
    }

    public InitialBipartition performPartitionBasedOnQuartet(Row quartetRow){
        // int q1 = (int) quartet_under_consideration.getList(0).get(0);
        // int q2 = (int) quartet_under_consideration.getList(0).get(1);
        // int q3 = (int) quartet_under_consideration.getList(1).get(0);
        // int q4 = (int) quartet_under_consideration.getList(1).get(1);

        Quartet quartet_under_consideration = new Quartet(quartetRow);
        int q1 = quartet_under_consideration.taxa_sisters_left[0];
        int q2 = quartet_under_consideration.taxa_sisters_left[1];
        int q3 = quartet_under_consideration.taxa_sisters_right[0];
        int q4 = quartet_under_consideration.taxa_sisters_right[1];

        int status_q1, status_q2, status_q3, status_q4; //status of q1,q2,q3,q4 respectively
        status_q1 = partitionMap.get(q1);
        status_q2 = partitionMap.get(q2);
        status_q3 = partitionMap.get(q3);
        status_q4 = partitionMap.get(q4);

        if (status_q1 == DefaultValues.UNASSIGNED_PARTITION && status_q2 == DefaultValues.UNASSIGNED_PARTITION /*all taxa of this quartet are unassigned to any bipartition*/
                && status_q3 == DefaultValues.UNASSIGNED_PARTITION && status_q4 == DefaultValues.UNASSIGNED_PARTITION) { // assign q1,q2 to left and q3,q4 to right
            partitionMap.put(q1, DefaultValues.LEFT_PARTITION);
            partitionMap.put(q2, DefaultValues.LEFT_PARTITION);
            partitionMap.put(q3, DefaultValues.RIGHT_PARTITION);
            partitionMap.put(q4, DefaultValues.RIGHT_PARTITION);
            count_taxa_left_partition += 2;
            count_taxa_right_partition += 2;
        }
        else {
            if (status_q1 == DefaultValues.UNASSIGNED_PARTITION) { //q1 not present in any partition

                if (status_q2 != DefaultValues.UNASSIGNED_PARTITION) { // if status_q2 is assignedlook for q2's partition. put q1 in there
                    if (status_q2 == DefaultValues.LEFT_PARTITION) {
                        status_q1 = DefaultValues.LEFT_PARTITION;
                        partitionMap.put(q1, DefaultValues.LEFT_PARTITION);
                        count_taxa_left_partition++;
                    } else {
                        status_q1 = DefaultValues.RIGHT_PARTITION;
                        partitionMap.put(q1, DefaultValues.RIGHT_PARTITION);
                        count_taxa_right_partition++;
                    }
                } //q3 is assgined
                else if (status_q3 != DefaultValues.UNASSIGNED_PARTITION) {
                    // q3 in left, put q1 in right
                    if (status_q3 == DefaultValues.LEFT_PARTITION) {
                        status_q1 = DefaultValues.RIGHT_PARTITION;
                        partitionMap.put(q1, DefaultValues.RIGHT_PARTITION);
                        count_taxa_right_partition++;
                    } // status_q3 in right,put status_q1 in left
                    else {
                        status_q1 = DefaultValues.LEFT_PARTITION;
                        partitionMap.put(q1, DefaultValues.LEFT_PARTITION);
                        count_taxa_left_partition++;
                    }
                } else if (status_q4 != DefaultValues.UNASSIGNED_PARTITION) {
                    // q4 in left, put q1 in right
                    if (status_q4 == DefaultValues.LEFT_PARTITION) {
                        status_q1 = DefaultValues.RIGHT_PARTITION;
                        partitionMap.put(q1, DefaultValues.RIGHT_PARTITION);
                        count_taxa_right_partition++;
                    } //q4 in right,put q1 in left
                    else {
                        status_q1 = DefaultValues.LEFT_PARTITION;
                        partitionMap.put(q1, DefaultValues.LEFT_PARTITION);
                        count_taxa_left_partition++;
                    }
                }

            }
            if (status_q2 == DefaultValues.UNASSIGNED_PARTITION) {
                //look for q1's partition, put q2 in there
                if (status_q1 == DefaultValues.LEFT_PARTITION) {
                    status_q2 = DefaultValues.LEFT_PARTITION;
                    partitionMap.put(q2, DefaultValues.LEFT_PARTITION);
                    count_taxa_left_partition++;
                } else {
                    status_q2 = DefaultValues.RIGHT_PARTITION;
                    partitionMap.put(q2, DefaultValues.RIGHT_PARTITION);
                    count_taxa_right_partition++;
                }

            }
            if (status_q3 == DefaultValues.UNASSIGNED_PARTITION) {
                if (status_q4 != DefaultValues.UNASSIGNED_PARTITION) //q4 is assigned, look for q4 and put q3 in there
                {
                    if (status_q4 == DefaultValues.RIGHT_PARTITION) {
                        status_q3 = DefaultValues.RIGHT_PARTITION;
                        partitionMap.put(q3, DefaultValues.RIGHT_PARTITION);
                        count_taxa_right_partition++;
                    } else {
                        status_q3 = DefaultValues.LEFT_PARTITION;
                        partitionMap.put(q3, DefaultValues.LEFT_PARTITION);
                        count_taxa_left_partition++;
                    }
                } else {
                    if (status_q1 == DefaultValues.RIGHT_PARTITION) {
                        status_q3 = DefaultValues.LEFT_PARTITION;
                        partitionMap.put(q3, DefaultValues.LEFT_PARTITION);
                        count_taxa_left_partition++;
                    } else {
                        status_q3 = DefaultValues.RIGHT_PARTITION;
                        partitionMap.put(q3, DefaultValues.RIGHT_PARTITION);
                        count_taxa_right_partition++;
                    }
                }
            }
            if (status_q4 == DefaultValues.UNASSIGNED_PARTITION) {
                if (status_q3 == DefaultValues.LEFT_PARTITION) {
                    status_q4 = DefaultValues.LEFT_PARTITION;
                    partitionMap.put(q4, DefaultValues.LEFT_PARTITION);
                    count_taxa_left_partition++;
                } else {
                    status_q4 = DefaultValues.RIGHT_PARTITION;
                    partitionMap.put(q4, DefaultValues.RIGHT_PARTITION);
                    count_taxa_right_partition++;
                }

            }
        } //done going through all quartets
        return this;
    }
    public Map<Integer, Integer> performPartitionRandomBalanced(Map<Integer, Integer> partitionMap) {

        //this is not the way
        // List<Quartet> quartetList = quartetDataset.collectAsList();

        // for (Quartet quartet_under_consideration : quartetList) {
        //     performPartitionBasedOnQuartet(quartet_under_consideration);
        // }

        //now assign remaining taxa randomly step4
        int flag_for_random_assignment = 0;
        for (int key_tax : partitionMap.keySet()) {
            if (partitionMap.get(key_tax) == DefaultValues.UNASSIGNED_PARTITION) {
                if (count_taxa_left_partition < count_taxa_right_partition) {
                    flag_for_random_assignment = 2;
                } else if (count_taxa_left_partition > count_taxa_right_partition) {
                    flag_for_random_assignment = 1;
                } else {
                    flag_for_random_assignment++;
                }
                if (flag_for_random_assignment % 2 == 0) {
                    partitionMap.put(key_tax, DefaultValues.LEFT_PARTITION);
                    count_taxa_left_partition++;
                } else {
                    partitionMap.put(key_tax, DefaultValues.RIGHT_PARTITION);
                    count_taxa_right_partition++;
                }
            }
        }

        return partitionMap;

    }

}
