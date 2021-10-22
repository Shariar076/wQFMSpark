package legacy.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InitialTable {
    public static Map<String, Integer> map_of_str_vs_int_tax_list = new HashMap<>(); //for forward checking
    public static Map<Integer, String> map_of_int_vs_str_tax_list = new HashMap<>(); //for back conversion
    public static int TAXA_COUNTER = 0;

    private List<Quartet> list_quartets;

    public InitialTable() {
        this.list_quartets = new ArrayList<Quartet>();
    }
    public InitialTable(boolean flag) {
        // do not initialize. [to pass as reference]

    }
    public int sizeTable() {
        return list_quartets.size();
    }

    public void addToListOfQuartets(Quartet q) {
        this.list_quartets.add(q);
    }

    @Override
    public String toString() {
        return "InitialTable{" + "list_quartets=" + list_quartets + '}';
    }

    public Quartet get(int idx) {
        return list_quartets.get(idx);
    }
    public void assignByReference(InitialTable initialTable) {
        this.list_quartets = initialTable.list_quartets; //assign by reference
    }
}
