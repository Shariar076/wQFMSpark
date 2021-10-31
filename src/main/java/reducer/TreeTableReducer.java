package reducer;

import org.apache.spark.api.java.function.ReduceFunction;
import phylonet.tree.model.sti.STINode;
import phylonet.tree.model.sti.STITree;
import structure.TreeTable;

import java.util.*;
import java.util.regex.Pattern;

public class TreeTableReducer implements ReduceFunction<TreeTable> {
    public static ArrayList<String> TAXA_LIST;

    public TreeTableReducer(ArrayList<String> TAXA_LIST) {
        TreeTableReducer.TAXA_LIST = TAXA_LIST;
    }

    public static List<String> iteratorToList(Iterator<STINode> iterator) {
        List<String> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next().toString());
        }
        return list;
    }

    private static int countCharInString(String str, char c) {
        int count = 0;
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == c) {
                count++;
            }
        }
        return count;
    }

    public static boolean ifTaxonIsLeaf(STINode node, String taxon) {
        String newickTree = node.toNewick();
        String tempStr = newickTree.substring(0, newickTree.indexOf(taxon));
        int numLeftParenUptoTaxon = countCharInString(tempStr, '(');
        return numLeftParenUptoTaxon == 1;
    }

    public static String updateMainTree(STITree mainTree, STINode updateSubtree, List<String> updateLeaves, String absentTaxon) throws Exception {
        STITree copyMainTree = new STITree(mainTree);
        String mainTreeNewick = mainTree.toNewick();
        // System.out.println("Converted to Str: " + mainTreeNewick);

        updateLeaves.remove(absentTaxon);
        Iterable<String> iterable = updateLeaves;
        copyMainTree.constrainByLeaves(iterable);
        //System.out.println("After constraint: " + mainTreeNewick);


        List<String> constrainedMainTreeLeaves = Arrays.asList(copyMainTree.getLeaves());
        Collections.sort(constrainedMainTreeLeaves);
        Collections.sort(updateLeaves);
        // System.out.println("constrainedMainTreeLeaves: " + constrainedMainTreeLeaves);
        // System.out.println("updateLeaves: " + updateLeaves);
        if (!constrainedMainTreeLeaves.equals((updateLeaves)))
            throw new Exception("Discordance: subtree mismatch with leaves" + updateLeaves);

        String searchStr = copyMainTree.toNewick();
        searchStr = searchStr.substring(0, searchStr.indexOf(";"));
        // System.out.println("searchStr: " + searchStr);
        String replaceStr = updateSubtree.toNewick();
        replaceStr = replaceStr.substring(0, replaceStr.indexOf(";"));
        // System.out.println("replaceStr: " + replaceStr);
        String prevStr = mainTreeNewick;
        mainTreeNewick = mainTreeNewick.replaceAll(Pattern.quote(searchStr), replaceStr);

        if (prevStr.equals(mainTreeNewick))
            throw new Exception("Error: " + searchStr + " Not found; check for discordance");

        // System.out.println("final newick: " + mainTreeNewick);

        return mainTreeNewick;
    }

    public static String addSubtreesWithMissingTaxa(STITree stiTree, STITree t1) throws Exception {
        List<String> taxa_list = TAXA_LIST;

        List<String> absentTaxa = new ArrayList<>();
        for (String taxon : taxa_list) {
            if (!Arrays.asList(stiTree.getLeaves()).contains(taxon)) absentTaxa.add(taxon);
        }
        // System.out.println("absentTaxa: " + absentTaxa);

        if (absentTaxa.size() > 0) {
            List<STINode> targetNode = new ArrayList<>();

            int found = 0;
            for (int i = 0; i < t1.getNodeCount(); i++) {
                STINode thisNode = t1.getNode(i);
                List<String> thisNodeLeaves = iteratorToList(thisNode.getLeaves().iterator());
                // assumption: The first node found with any absent taxa should contain all absent taxa
                for (String taxon : absentTaxa) {
                    if (thisNodeLeaves.contains(taxon) && ifTaxonIsLeaf(thisNode, taxon)) {
                        //find the position of missing taxa subtree;
                        //replace with new sub tree with missing taxa
                        String updatedStiTree = updateMainTree(stiTree, thisNode, thisNodeLeaves, taxon);

                        stiTree = new STITree(updatedStiTree);
                        // System.out.println("thisNode: " + thisNode);
                        targetNode.add(thisNode);
                        found++;
                    }
                }
                if (found == absentTaxa.size()) break;
            }
        }
        return stiTree.toNewick();

    }

    @Override
    public TreeTable call(TreeTable treeTable, TreeTable t1) throws Exception {
        STITree newickTree1 = null;
        STITree newickTree2 = null;
        newickTree1 = new STITree(treeTable.tree);
        newickTree2 = new STITree(t1.tree);

        if (treeTable.tree == null) {
            return t1;
        } else {
            String updatedStr;
            try {
                updatedStr = addSubtreesWithMissingTaxa(newickTree1, newickTree2);
            }catch (Exception e){
                e.printStackTrace();
                return treeTable;
            }
            // System.out.println(updatedStr);
            treeTable.setTree(updatedStr);
            treeTable.setTag(treeTable.getTag() + "+" + t1.getTag());
            treeTable.setSupport(treeTable.getSupport() + t1.getSupport());
            return treeTable;
        }
    }
}
