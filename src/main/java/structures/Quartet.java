package structures;

import java.util.Arrays;

public class Quartet {
    public static int NUM_TAXA_PER_PARTITION = 2;
    public static int TEMP_TAX_TO_SWAP;

    public int[] taxa_sisters_left;// = new String[NUM_TAXA_PER_PARTITION];
    public int[] taxa_sisters_right;// = new String[NUM_TAXA_PER_PARTITION];
    public double weight;

    public Quartet() {
        this.weight = 1.0;
    }


    public Quartet(int a, int b, int c, int d, double w) {
        initialiseQuartet(a, b, c, d, w);
    }
    public Quartet(Quartet q) {
        initialiseQuartet(q.taxa_sisters_left[0], q.taxa_sisters_left[1],
                q.taxa_sisters_right[0], q.taxa_sisters_right[1], q.weight);
    }
    public void sort_quartet_taxa_names() {
//        String[] left = {this.taxa_sisters_left[0], this.taxa_sisters_left[1]};
//        String[] right = {this.taxa_sisters_right[0], this.taxa_sisters_right[1]};

        Arrays.sort(this.taxa_sisters_left);
        Arrays.sort(this.taxa_sisters_right);

        if (this.taxa_sisters_left[0] < this.taxa_sisters_right[0]) { //don't swap two sides
            //no need to swap
        } else {  // swap two sides
            for (int i = 0; i < Quartet.NUM_TAXA_PER_PARTITION; i++) {
                Quartet.TEMP_TAX_TO_SWAP = this.taxa_sisters_left[i];
                this.taxa_sisters_left[i] = this.taxa_sisters_right[i];
                this.taxa_sisters_right[i] = Quartet.TEMP_TAX_TO_SWAP;
            }
        }
    }

    public final void initialiseQuartet(int a, int b, int c, int d, double w) {
        //sorting.

        this.taxa_sisters_left = new int[NUM_TAXA_PER_PARTITION];
        this.taxa_sisters_right = new int[NUM_TAXA_PER_PARTITION];

        this.taxa_sisters_left[0] = a;
        this.taxa_sisters_left[1] = b;
        this.taxa_sisters_right[0] = c;
        this.taxa_sisters_right[1] = d;
        this.weight = w;

        this.sort_quartet_taxa_names();

    }

    public static int getNumTaxaPerPartition() {
        return NUM_TAXA_PER_PARTITION;
    }

    public static int getTempTaxToSwap() {
        return TEMP_TAX_TO_SWAP;
    }

    public int[] getTaxa_sisters_left() {
        return taxa_sisters_left;
    }

    public int[] getTaxa_sisters_right() { return taxa_sisters_right; }

    public double getWeight() { return weight; }

    public static void setNumTaxaPerPartition(int numTaxaPerPartition) {
        NUM_TAXA_PER_PARTITION = numTaxaPerPartition;
    }

    public static void setTempTaxToSwap(int tempTaxToSwap) {
        TEMP_TAX_TO_SWAP = tempTaxToSwap;
    }

    public void setTaxa_sisters_left(int[] taxa_sisters_left) {
        this.taxa_sisters_left = taxa_sisters_left;
    }

    public void setTaxa_sisters_right(int[] taxa_sisters_right) {
        this.taxa_sisters_right = taxa_sisters_right;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    @Override
    public String toString() {
        String s = "((" + this.taxa_sisters_left[0] + "," + this.taxa_sisters_left[1] + "),(" + this.taxa_sisters_right[0] + "," + this.taxa_sisters_right[1] + ")); " + String.valueOf(this.weight);
        return s;
    }
    @Override
    public int hashCode() {
        int hash = 3;
        hash = 97 * hash + Arrays.hashCode(this.taxa_sisters_left);
        hash = 97 * hash + Arrays.hashCode(this.taxa_sisters_right);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Quartet other = (Quartet) obj;
        other.sort_quartet_taxa_names();

        if (!Arrays.equals(this.taxa_sisters_left, other.taxa_sisters_left)) {
            return false;
        }
        if (!Arrays.equals(this.taxa_sisters_right, other.taxa_sisters_right)) {
            return false;
        }
        return true;
    }
}
