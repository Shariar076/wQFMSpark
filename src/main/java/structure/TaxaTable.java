package structure;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.sql.Dataset;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TaxaTable implements Serializable {
    public int TAXA_COUNT = 0;
    public ArrayList<String> TAXA_LIST = new ArrayList<>();

    // public Map<String,Integer> TAXA_GRP_COUNT = new HashMap<>();

    // public ArrayList<ArrayList<String>> TAXA_PARTITION_LIST =  new ArrayList<>();

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

    // public ArrayList<ArrayList<String>> getTAXA_PARTITION_LIST() {
    //     return TAXA_PARTITION_LIST;
    // }
    //
    // public void setTAXA_PARTITION_LIST(ArrayList<ArrayList<String>> TAXA_PARTITION_LIST) {
    //     this.TAXA_PARTITION_LIST = TAXA_PARTITION_LIST;
    // }

    @Override
    public String toString() {
        return "TaxaTable{" +
                "TAXA_COUNT=" + TAXA_COUNT +
                ",\n TAXA_LIST=" + TAXA_LIST +
                // ",\n TAXA_PARTITION_LIST=" + TAXA_PARTITION_LIST +
                '}';
    }
}
