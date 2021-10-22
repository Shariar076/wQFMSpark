package legacy.feat;

import legacy.configs.Config;
import legacy.configs.DefaultValues;

import java.util.ArrayList;
import java.util.List;

public class Bin {
    public static double proportion_left_thresh;
    public static double proportion_after_thresh_before_1;
    public static double proportion_greater_or_equal_1;
    public static boolean WILL_DO_DYNAMIC = true;

    private double lower_limit;
    private double upper_limit;
    private double frequency; // no need for a map/dictionary

    public Bin(double lower_lim, double upper_lim) {
        this.lower_limit = lower_lim;
        this.upper_limit = upper_lim;
        this.frequency = 0;
    }


    public static List<Bin> getListOfBins(double lower_limit, double upper_limit, double step_size) {
        List<Bin> bins = new ArrayList<>();
        double lower_iter = lower_limit;
        while (lower_iter < upper_limit) {
            bins.add(new Bin(lower_iter, (lower_iter + step_size)));
            lower_iter += step_size;
        }
        return bins;
    }

    private static boolean does_lie_within(double left, double right, double val) {
        return val >= left && val < right;
    }

    private static boolean is_within_bin(Bin bin, double ratio) {
        return does_lie_within(bin.lower_limit, bin.upper_limit, ratio);
    }

    private double getMidPoint() {
        // may be later can be used as some other way of finding the limiting point instead of direct mid-point.
        return 0.5 * (this.lower_limit + this.upper_limit);
    }

    public static double calculateBinsAndFormScores(List<Double> list_ratios) {

        int cnt_before_thresh = 0;
        int cnt_after_thresh_before_1 = 0;
        int cnt_after_1 = 0;

        //already counts are initialized to 0
        List<Bin> bins = Bin.getListOfBins(0.5, 1.0, Config.STEP_SIZE_BINNING); //initialize bins from [0.5,1.0] in delta = 0.01
//        System.out.println(bins);

        //calculate counts in each bin as well as the proportion-counts
        if (bins.isEmpty()) {
            System.out.println("-->>L 64. Num of bins is empty. Returning default beta = " + DefaultValues.BETA_DEFAULT_VAL);
            return DefaultValues.BETA_DEFAULT_VAL;
        }
        double upper_limit_of_highest_bin = bins.get(bins.size() - 1).upper_limit;

        //proportion-counts and bin-counts for each ratio and each bin.
        for (double ratio : list_ratios) {
            //proportion counts ..
            if (Bin.does_lie_within(0.5, Config.THRESHOLD_BINNING, ratio)) {
                cnt_before_thresh++;
            } else if (Bin.does_lie_within(Config.THRESHOLD_BINNING, upper_limit_of_highest_bin, ratio)) {
                cnt_after_thresh_before_1++;
            } else { // >= 1
                cnt_after_1++;
            }
            //bin's counts
            for (Bin bin : bins) {
                if (Bin.is_within_bin(bin, ratio) == true) {
                    bin.frequency++; //if ratio lies in this bin, increment
                }
            }
        }

        //find-proportions.
        int total_count = cnt_before_thresh + cnt_after_thresh_before_1 + cnt_after_1;

        //base-case if no ratios exist. [should be handled from calling function, but check nonetheless]
        if (total_count == 0) {
            System.out.println("L 97. Bin. Total-Count-Ratios = 0, Use beta default = " + DefaultValues.BETA_DEFAULT_VAL);
            return DefaultValues.BETA_DEFAULT_VAL;
        }
        //set-up the proportions accordingly.
        Bin.proportion_left_thresh = (double) cnt_before_thresh / (double) total_count;
        Bin.proportion_after_thresh_before_1 = (double) cnt_after_thresh_before_1 / (double) total_count;
        Bin.proportion_greater_or_equal_1 = (double) cnt_after_1 / (double) total_count;

//        System.out.printf("L 101. cnt_before_thresh = %d, cnt_after_thresh_before_1 = %d, cnt_after_1 = %d, total_count = %d"
//                , cnt_before_thresh, cnt_after_thresh_before_1, cnt_after_1, total_count);

        double weighted_avg_final; //this will be passed as BETA

        if (Bin.proportion_left_thresh >= Config.CUT_OFF_LIMIT_BINNING) { //greater than cut-off so, bin(left, thresh)
            //bin on the left side.
            double cumulative_mid_point_product_counts = 0;
            for (Bin bin : bins) {
                if (bin.lower_limit < Config.THRESHOLD_BINNING) { //Bin from 0.5 upto 0.9 [left-side-bin]
                    //sum the mid-point-of-class * frequency-of-class
                    cumulative_mid_point_product_counts += (bin.getMidPoint() * bin.frequency);
                }
            }
            weighted_avg_final = (cumulative_mid_point_product_counts) / (double) cnt_before_thresh;
        } //bin on the right side.
        else {
            double cumulative_mid_point_product_counts = 0;
            for (Bin bin : bins) {
                if (bin.lower_limit >= Config.THRESHOLD_BINNING) {
                    cumulative_mid_point_product_counts += (bin.getMidPoint() * bin.frequency);
                }
            }
///             compute using both bins.
            if (Config.SET_RIGHT_TO_1 == true) {
                weighted_avg_final = DefaultValues.BETA_DEFAULT_VAL;
            } else {
                weighted_avg_final = (cumulative_mid_point_product_counts + (double) cnt_after_1) / ((double) (cnt_after_thresh_before_1 + cnt_after_1));
            }


        }
        bins.clear(); ///clear for gc to collect [WILL have to be more efficient than this]
        return weighted_avg_final;

    }

    @Override
    public String toString() {
        return "Bin(" + this.lower_limit + "," + this.upper_limit + "," + this.frequency + ")";
    }


}
