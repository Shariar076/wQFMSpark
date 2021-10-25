package wqfm.utils;

import wqfm.configs.DefaultValues;
import wqfm.ds.InitialTable;

import java.util.Map;

public class TaxaUtils {
    public static int findQuartetStatus(int left_sis1_bip, int left_sis2_bip, int right_sis1_bip, int right_sis2_bip) {
        int[] four_bipartitions = {left_sis1_bip, left_sis2_bip, right_sis1_bip, right_sis2_bip};

        int sum_four_bipartitions = IOHandler.sumArray(four_bipartitions);
        //Blank check: Easier to check if blank quartet (all four are same) [priority wise first]
//        if ((left_sisters_bip[0] == left_sisters_bip[1]) && (right_sisters_bip[0] == right_sisters_bip[1]) && (left_sisters_bip[0] == right_sisters_bip[0])) {

        if (Math.abs(sum_four_bipartitions) == 4) { // -1,-1,-1,-1 or +1,+1,+1,+1 all will lead to sum == 4
            return DefaultValues.BLANK;
        }
        //Deferred Check: sum == 2 check [otherwise, permutations will be huge]
        if (Math.abs(sum_four_bipartitions) == 2) { //-1,+1 ,+1,+1  => +2 or +1,-1 , -1,-1 => -2
            return DefaultValues.DEFERRED;
        }
        //Satisfied check: left are equal, right are equal AND left(any one) != right(any one)
        if ((left_sis1_bip == left_sis2_bip) && (right_sis1_bip == right_sis2_bip) && (left_sis1_bip != right_sis1_bip)) {
            return DefaultValues.SATISFIED;
        }
        //All check fails, Violated quartet
        return DefaultValues.VIOLATED;
    }
    public static int getDummyTaxonName(int level) {
        return (InitialTable.TAXA_COUNTER + level); //0->47 [original tax], then 48 and above ar DUMMY taxa
    }

    public static int getOppositePartition(int partition) {
        switch (partition) {
            case DefaultValues.LEFT_PARTITION:
                return DefaultValues.RIGHT_PARTITION;
            case DefaultValues.RIGHT_PARTITION:
                return DefaultValues.LEFT_PARTITION;
            default:
                return DefaultValues.UNASSIGNED_PARTITION;
        }
    }

    public static boolean isThisSingletonBipartition(Map<Integer, Integer> mapInitialBip) {
        int len = mapInitialBip.keySet().size();
        int sum = IOHandler.sumMapValuesInteger(mapInitialBip);

        return (Math.abs(sum) == (len - 2)) || (Math.abs(sum) == len);
    }

    public static int findQuartetStatusUsingShortcut(int status_quartet_before_hyp_swap) {
        if (status_quartet_before_hyp_swap == DefaultValues.DEFERRED) {
            return DefaultValues.UNKNOWN; //only if deferred, next calculations are necessary
        }
        return DefaultValues.DEFERRED; //s->d, v->d, b->d
    }

}
