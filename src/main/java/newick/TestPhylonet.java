package util;
import phylonet.tree.io.ParseException;
import phylonet.tree.model.sti.STITree;

import java.io.IOException;

public class TestPhylonet {
    public static void main(String[] args) {
        String newickStr1 = "(((10,8,9,7),(6,5)),(4,(3,(1,2))));";
        String newickStr2 = "(((10,8,9,7),(6,5)),(4,(3,(1,2))));";
        String newickStr3 = "((4,(3,(1,2))),((10,8,9,7),(6,5)));";
        STITree sampleNewickTree1 =null;
        STITree sampleNewickTree2 =null;
        STITree sampleNewickTree3 =null;
        try {
            sampleNewickTree1 = new STITree(newickStr1);
            sampleNewickTree2 = new STITree(newickStr2);
            sampleNewickTree3 = new STITree(newickStr3);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println(sampleNewickTree1);
        System.out.println(sampleNewickTree2);
        System.out.println(sampleNewickTree3);
        System.out.println(sampleNewickTree1.toNewick().equals(sampleNewickTree2.toNewick()));
        System.out.println(sampleNewickTree1.toNewick().equals(sampleNewickTree3.toNewick()));
    }
}
