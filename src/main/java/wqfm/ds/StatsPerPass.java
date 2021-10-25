package wqfm.ds;

import java.util.Map;
import wqfm.bip.Bipartition8Values;

/**
 *
 * @author mahim
 */
public class StatsPerPass {

    public int whichTaxaWasPassed;
    public double maxGainOfThisPass;
    public Bipartition8Values _8_values_chosen_for_this_pass;
    public final Map<Integer, Integer> map_final_bipartition;

    public StatsPerPass(int whichTaxaWasPassed, double maxGainOfThisPass,
                        Bipartition8Values _8_vals,
                        Map<Integer, Integer> map) {
        this.whichTaxaWasPassed = whichTaxaWasPassed;
        this.maxGainOfThisPass = maxGainOfThisPass;
        this._8_values_chosen_for_this_pass = new Bipartition8Values(_8_vals);
        this.map_final_bipartition = map; // this also works.
//        this.map_final_bipartition = new HashMap<>(map);

    }

    @Override
    public String toString() {
        return "StatsPerPass{" + "whichTaxaWasPassed=" + whichTaxaWasPassed + ", maxGainOfThisPass=" + maxGainOfThisPass + ", _8_values_chosen_for_this_pass=" + _8_values_chosen_for_this_pass + ", map_final_bipartition=" + map_final_bipartition + '}';
    }

}