package reducer;

import phylonet.tree.io.ParseException;
import phylonet.tree.model.sti.STINode;
import phylonet.tree.model.sti.STITree;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class TreeReducer {
    public static ArrayList<String> TAXA_LIST;

    public TreeReducer(ArrayList<String> TAXA_LIST) {
        TreeReducer.TAXA_LIST = TAXA_LIST;
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
        // Collections.sort(constrainedMainTreeLeaves);
        // Collections.sort(updateLeaves);
        if (!updateLeaves.containsAll(constrainedMainTreeLeaves))
            throw new Exception("Discordance: subtree mismatch between constrainedMainTreeLeaves: " +
                    constrainedMainTreeLeaves + " and updateLeaves:" + updateLeaves);

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

    // @Override
    public String call(String tree, String t1) { // throws Exception

        if (tree == null) {
            return t1;
        } else {
            STITree newickTree1 = null;
            STITree newickTree2 = null;
            String updatedStr ="";
            try {
                newickTree1 = new STITree(tree);
                newickTree2 = new STITree(t1);
                updatedStr = addSubtreesWithMissingTaxa(newickTree1, newickTree2);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("TreeReducer tree: " + tree);
                System.out.println("TreeReducer t1: " + t1);
            }
            return updatedStr;
        }
    }

    // public static void main(String[] args) {
    //     // TAXA_LIST=[1, 5, 8, 9, 2, 4, 7, 6, 11, 3, 10]
    //     ArrayList<String> taxa_list = new ArrayList<>(Arrays.asList(new String[]{"1", "5", "8", "9", "2", "4", "7", "6", "11", "3", "10"}));
    //     TreeReducer tr = new TreeReducer(taxa_list);
    //     try {
    //         tr.call("((11,(4,(3,(1,2)))),((5,6),(9,(8,7))));", "((10,((5,6),(9,(8,7)))),(11,(4,(3,(1,2)))));");
    //     } catch (Exception e) {
    //         e.printStackTrace();
    //     }
    // }
}
