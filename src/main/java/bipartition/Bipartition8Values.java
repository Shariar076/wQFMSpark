package bipartition;

import config.Config;
import config.DefaultValues;
import feature.FeatureComputer;
import org.apache.spark.sql.Row;
import structure.Quartet;
import util.TaxaUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Bipartition8Values implements Serializable {
    public int numSatisfied;
    public int numViolated;
    public int numDeferred;
    public int numBlank;

    public double wtSatisfied;
    public double wtViolated;
    public double wtDeferred;
    public double wtBlank;
    public Map<List<Integer>, List<Double>> map_four_tax_seq_weights_list;
    public Map<Integer, Integer> initialBipartitionMap;

    public Bipartition8Values(Map<Integer, Integer> initialBipartitionMap) {
        this.numSatisfied = 0;
        this.numViolated = 0;
        this.numDeferred = 0;
        this.numBlank = 0;
        this.wtSatisfied = 0.0;
        this.wtViolated = 0.0;
        this.wtDeferred = 0.0;
        this.wtBlank = 0.0;
        this.map_four_tax_seq_weights_list = new HashMap<>();
        this.initialBipartitionMap =initialBipartitionMap;

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
        this.map_four_tax_seq_weights_list = obj.map_four_tax_seq_weights_list;
        this.initialBipartitionMap = obj.initialBipartitionMap;
    }


    public String getScoresAsString(){
        List<Integer> scoresNumbered = Arrays.asList(numSatisfied, numViolated, numDeferred, numBlank);
        List<Double> scoresWeighted = Arrays.asList(wtSatisfied, wtViolated, wtDeferred, wtBlank);
        return scoresNumbered +" "+ scoresWeighted;
    }
    public Bipartition8Values compute8ValuesUsingAllQuartets_this_level(Row quartetRow) {
        Quartet quartet = new Quartet(quartetRow);

        if (Config.PARTITION_SCORE_MODE == DefaultValues.PARTITION_SCORE_FULL_DYNAMIC) {
            FeatureComputer.makeDictionary(quartet, map_four_tax_seq_weights_list);
        }

        //obtain the quartet's taxa's bipartitions
        int left_sis_1_bip_val = initialBipartitionMap.get(quartet.taxa_sisters_left[0]);
        int left_sis_2_bip_val = initialBipartitionMap.get(quartet.taxa_sisters_left[1]);
        int right_sis_1_bip_val = initialBipartitionMap.get(quartet.taxa_sisters_right[0]);
        int right_sis_2_bip_val = initialBipartitionMap.get(quartet.taxa_sisters_right[1]);

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
        return this;

    }

    public void calculateDynamicScore(int level) {
        if (Config.PARTITION_SCORE_MODE == DefaultValues.PARTITION_SCORE_FULL_DYNAMIC) {
            FeatureComputer.computeBinningFeature(map_four_tax_seq_weights_list, level);
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

    public int getNumSatisfied() {
        return numSatisfied;
    }

    public void setNumSatisfied(int numSatisfied) {
        this.numSatisfied = numSatisfied;
    }

    public int getNumViolated() {
        return numViolated;
    }

    public void setNumViolated(int numViolated) {
        this.numViolated = numViolated;
    }

    public int getNumDeferred() {
        return numDeferred;
    }

    public void setNumDeferred(int numDeferred) {
        this.numDeferred = numDeferred;
    }

    public int getNumBlank() {
        return numBlank;
    }

    public void setNumBlank(int numBlank) {
        this.numBlank = numBlank;
    }

    public double getWtSatisfied() {
        return wtSatisfied;
    }

    public void setWtSatisfied(double wtSatisfied) {
        this.wtSatisfied = wtSatisfied;
    }

    public double getWtViolated() {
        return wtViolated;
    }

    public void setWtViolated(double wtViolated) {
        this.wtViolated = wtViolated;
    }

    public double getWtDeferred() {
        return wtDeferred;
    }

    public void setWtDeferred(double wtDeferred) {
        this.wtDeferred = wtDeferred;
    }

    public double getWtBlank() {
        return wtBlank;
    }

    public void setWtBlank(double wtBlank) {
        this.wtBlank = wtBlank;
    }

    public Map<List<Integer>, List<Double>> getMap_four_tax_seq_weights_list() {
        return map_four_tax_seq_weights_list;
    }

    public void setMap_four_tax_seq_weights_list(Map<List<Integer>, List<Double>> map_four_tax_seq_weights_list) {
        this.map_four_tax_seq_weights_list = map_four_tax_seq_weights_list;
    }

    public Map<Integer, Integer> getInitialBipartitionMap() {
        return initialBipartitionMap;
    }

    public void setInitialBipartitionMap(Map<Integer, Integer> initialBipartitionMap) {
        this.initialBipartitionMap = initialBipartitionMap;
    }
}
