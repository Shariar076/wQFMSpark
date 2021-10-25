package legacy.ds;

import java.util.Arrays;

public class LegacyQuartet {
    public static int NUM_TAXA_PER_PARTITION = 2;
    public static int TEMP_TAX_TO_SWAP;

    public int[] taxa_sisters_left;// = new String[NUM_TAXA_PER_PARTITION];
    public int[] taxa_sisters_right;// = new String[NUM_TAXA_PER_PARTITION];
    public double weight;

    public LegacyQuartet() {
        this.weight = 1.0;
    }
    public LegacyQuartet(int a, int b, int c, int d, double w) {
        initialiseQuartet(a, b, c, d, w);
    }
    public LegacyQuartet(LegacyQuartet q) {
        initialiseQuartet(q.taxa_sisters_left[0], q.taxa_sisters_left[1],
                q.taxa_sisters_right[0], q.taxa_sisters_right[1], q.weight);
    }
    public LegacyQuartet(String s) {
        //ADDITIONALLY append to the map and reverse map.
        s = s.replace(" ", "");
        s = s.replace(";", ",");
        s = s.replace("(", "");
        s = s.replace(")", ""); // Finally end up with A,B,C,D,41.0
        String[] arr = s.split(",");
        int a, b, c, d;
        if (LegacyInitialTable.map_of_str_vs_int_tax_list.containsKey(arr[0]) == true) {
            a = LegacyInitialTable.map_of_str_vs_int_tax_list.get(arr[0]);
        } else { //THIS taxon doesn't exist.
            a = LegacyInitialTable.TAXA_COUNTER;
            LegacyInitialTable.TAXA_COUNTER++;
            LegacyInitialTable.map_of_str_vs_int_tax_list.put(arr[0], a);
            LegacyInitialTable.map_of_int_vs_str_tax_list.put(a, arr[0]);
        }

        if (LegacyInitialTable.map_of_str_vs_int_tax_list.containsKey(arr[1]) == true) {
            b = LegacyInitialTable.map_of_str_vs_int_tax_list.get(arr[1]);
        } else { //THIS taxon doesn't exist.
            b = LegacyInitialTable.TAXA_COUNTER;
            LegacyInitialTable.TAXA_COUNTER++;
            LegacyInitialTable.map_of_str_vs_int_tax_list.put(arr[1], b);
            LegacyInitialTable.map_of_int_vs_str_tax_list.put(b, arr[1]);
        }
        if (LegacyInitialTable.map_of_str_vs_int_tax_list.containsKey(arr[2]) == true) {
            c = LegacyInitialTable.map_of_str_vs_int_tax_list.get(arr[2]);
        } else { //THIS taxon doesn't exist.
            c = LegacyInitialTable.TAXA_COUNTER;
            LegacyInitialTable.TAXA_COUNTER++;
            LegacyInitialTable.map_of_str_vs_int_tax_list.put(arr[2], c);
            LegacyInitialTable.map_of_int_vs_str_tax_list.put(c, arr[2]);
        }
        if (LegacyInitialTable.map_of_str_vs_int_tax_list.containsKey(arr[3]) == true) {
            d = LegacyInitialTable.map_of_str_vs_int_tax_list.get(arr[3]);
        } else { //THIS taxon doesn't exist.
            d = LegacyInitialTable.TAXA_COUNTER;
            LegacyInitialTable.TAXA_COUNTER++;
            LegacyInitialTable.map_of_str_vs_int_tax_list.put(arr[3], d);
            LegacyInitialTable.map_of_int_vs_str_tax_list.put(d, arr[3]);
        }

        initialiseQuartet(a, b, c, d, Double.parseDouble(arr[4]));

//        initialiseQuartet(arr[0], arr[1], arr[2], arr[3], Double.parseDouble(arr[4]));
    }

    public void sort_quartet_taxa_names() {
//        String[] left = {this.taxa_sisters_left[0], this.taxa_sisters_left[1]};
//        String[] right = {this.taxa_sisters_right[0], this.taxa_sisters_right[1]};

        Arrays.sort(this.taxa_sisters_left);
        Arrays.sort(this.taxa_sisters_right);

        if (this.taxa_sisters_left[0] < this.taxa_sisters_right[0]) { //don't swap two sides
            //no need to swap
        } else {  // swap two sides
            for (int i = 0; i < LegacyQuartet.NUM_TAXA_PER_PARTITION; i++) {
                LegacyQuartet.TEMP_TAX_TO_SWAP = this.taxa_sisters_left[i];
                this.taxa_sisters_left[i] = this.taxa_sisters_right[i];
                this.taxa_sisters_right[i] = LegacyQuartet.TEMP_TAX_TO_SWAP;
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
        LegacyQuartet other = (LegacyQuartet) obj;
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