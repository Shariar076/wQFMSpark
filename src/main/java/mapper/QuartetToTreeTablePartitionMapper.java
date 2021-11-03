package mapper;

import algorithm.wQFMRunner;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import structure.TreeTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class QuartetToTreeTablePartitionMapper implements MapPartitionsFunction<Row, TreeTable> {
    @Override
    public Iterator<TreeTable> call(Iterator<Row> iterator) throws Exception {
        TreeTable treeTable = new TreeTable();
        int qtCount = 0;
        double qtWeightSum = 0;
        String qtTag = "";
        ArrayList<String> arrayList = new ArrayList<>();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            arrayList.add(row.getString(0));
            // qtCount++;
            qtWeightSum+=Double.parseDouble(row.getString(2)); //row.getDouble(2);
            if(qtTag.isEmpty()) qtTag = row.getString(1); // find this partition's tag
            else if (!qtTag.equals(row.getString(1))) throw new Exception("Tag mismatch within partition");
        }
        String tree = "<NULL>";
        if (arrayList.size() > 0) tree = new wQFMRunner().runDevideNConquer(arrayList, qtTag);

        treeTable.setTree(tree);
        treeTable.setTag(qtTag);
        treeTable.setSupport(qtWeightSum);
        return Collections.singletonList(treeTable).iterator();

    }
}
