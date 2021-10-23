package legacy.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LegacyInitialTable {
    public static Map<String, Integer> map_of_str_vs_int_tax_list = new HashMap<>(); //for forward checking
    public static Map<Integer, String> map_of_int_vs_str_tax_list = new HashMap<>(); //for back conversion
    public static int TAXA_COUNTER = 0;

    private List<LegacyQuartet> list_Legacy_quartets;

    public LegacyInitialTable() {
        this.list_Legacy_quartets = new ArrayList<LegacyQuartet>();
    }
    public LegacyInitialTable(boolean flag) {
        // do not initialize. [to pass as reference]

    }
    public int sizeTable() {
        return list_Legacy_quartets.size();
    }

    public void addToListOfQuartets(LegacyQuartet q) {
        this.list_Legacy_quartets.add(q);
    }

    @Override
    public String toString() {
        return "LegacyInitialTable{" + "list_Legacy_quartets=" + list_Legacy_quartets + '}';
    }

    public LegacyQuartet get(int idx) {
        return list_Legacy_quartets.get(idx);
    }
    public void assignByReference(LegacyInitialTable legacyInitialTable) {
        this.list_Legacy_quartets = legacyInitialTable.list_Legacy_quartets; //assign by reference
    }
}
