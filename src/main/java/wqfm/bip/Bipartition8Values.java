package wqfm.bip;

import wqfm.configs.Config;
import wqfm.configs.DefaultValues;
import wqfm.ds.CustomDSPerLevel;
import wqfm.ds.Quartet;
import wqfm.feat.FeatureComputer;
import wqfm.utils.TaxaUtils;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Bipartition8Values {
    public int numSatisfied;
    public int numViolated;
    public int numDeferred;
    public int numBlank;

    public double wtSatisfied;
    public double wtViolated;
    public double wtDeferred;
    public double wtBlank;

    public Bipartition8Values() {
        this.numSatisfied = 0;
        this.numViolated = 0;
        this.numDeferred = 0;
        this.numBlank = 0;
        this.wtSatisfied = 0.0;
        this.wtViolated = 0.0;
        this.wtDeferred = 0.0;
        this.wtBlank = 0.0;
    }
    public Bipartition8Values(Bipartition8Values obj) {
        this.numSatisfied = obj.numSatisfied;
        this.numViolated = obj.numViolated;
        this.numDeferred = obj.numDeferred;
        this.numBlank = obj.numBlank;
        this.wtSatisfied = obj.wtSatisfied;
        this.wtViolated = obj.wtViolated;
        this.wtDeferred = obj.wtDeferred;
        this.wtBlank = obj.wtBlank;
    }
    public void compute8ValuesUsingAllQuartets_this_level(CustomDSPerLevel customDS, Map<Integer, Integer> map_bipartitions) {

        Map<List<Integer>, List<Double>> map_four_tax_seq_weights_list = new HashMap<>();
        //  System.out.println("L 100. Bipartition_8_vals: BIPARTITION size : " + map_bipartitions.keySet().size());
        //  System.out.println("Keyset size before populating: " + dictiory_4Tax_sequence.keySet().size());
        //for feature computation

        for (int idx_quartet : customDS.quartet_indices_list_unsorted) {
            Quartet quartet = customDS.initial_table1_of_list_of_quartets.get(idx_quartet);

            if (Config.PARTITION_SCORE_MODE == DefaultValues.PARTITION_SCORE_FULL_DYNAMIC) {
                FeatureComputer.makeDictionary(quartet, map_four_tax_seq_weights_list);
            }

            //obtain the quartet's taxa's bipartitions
            int left_sis_1_bip_val = map_bipartitions.get(quartet.taxa_sisters_left[0]);
            int left_sis_2_bip_val = map_bipartitions.get(quartet.taxa_sisters_left[1]);
            int right_sis_1_bip_val = map_bipartitions.get(quartet.taxa_sisters_right[0]);
            int right_sis_2_bip_val = map_bipartitions.get(quartet.taxa_sisters_right[1]);

            int status_quartet = TaxaUtils.findQuartetStatus(left_sis_1_bip_val, left_sis_2_bip_val, right_sis_1_bip_val, right_sis_2_bip_val); //obtain quartet status
            //compute scores according to status.
            switch (status_quartet) {
                case DefaultValues.SATISFIED:
                    this.numSatisfied++;
                    this.wtSatisfied += quartet.weight;
                    break;
                case DefaultValues.VIOLATED:
                    this.numViolated++;
                    this.wtViolated += quartet.weight;
                    break;
                case DefaultValues.DEFERRED:
                    this.numDeferred++;
                    this.wtDeferred += quartet.weight;
                    break;
                case DefaultValues.BLANK:
                    this.numBlank++;
                    this.wtBlank += quartet.weight;
                    break;
                default:
                    break;
            }
        }

        if (Config.PARTITION_SCORE_MODE == DefaultValues.PARTITION_SCORE_FULL_DYNAMIC) {
            FeatureComputer.computeBinningFeature(map_four_tax_seq_weights_list, customDS.level);
        }

    }

    public void addRespectiveValue(double weight, int status) {
        switch (status) {
            case DefaultValues.SATISFIED:
                this.numSatisfied++;
                this.wtSatisfied += weight;
                break;
            case DefaultValues.VIOLATED:
                this.numViolated++;
                this.wtViolated += weight;
                break;
            case DefaultValues.DEFERRED:
                this.numDeferred++;
                this.wtDeferred += weight;
                break;
            case DefaultValues.BLANK:
                this.numBlank++;
                this.wtBlank += weight;
                break;
            case DefaultValues.UNKNOWN: // do nothing for this case
                break;
            default:
                break;
        }
    }

    public void addObject(Bipartition8Values obj) {
        this.numSatisfied += obj.numSatisfied;
        this.numViolated += obj.numViolated;
        this.numDeferred += obj.numDeferred;
        this.numBlank += obj.numBlank;
        this.wtSatisfied += obj.wtSatisfied;
        this.wtViolated += obj.wtViolated;
        this.wtDeferred += obj.wtDeferred;
        this.wtBlank += obj.wtBlank;
    }

    public void subtractObject(Bipartition8Values obj) {
        this.numSatisfied -= obj.numSatisfied;
        this.numViolated -= obj.numViolated;
        this.numDeferred -= obj.numDeferred;
        this.numBlank -= obj.numBlank;
        this.wtSatisfied -= obj.wtSatisfied;
        this.wtViolated -= obj.wtViolated;
        this.wtDeferred -= obj.wtDeferred;
        this.wtBlank -= obj.wtBlank;
    }

}
