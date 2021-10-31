package newick;

import phylonet.tree.io.ParseException;
import phylonet.tree.model.sti.STINode;
import phylonet.tree.model.sti.STITree;
import structure.TreeTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class TestPhylonet {

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

    public static String updateMainTree(STITree mainTree, STINode updateSubtree, List<String> updateLeaves, String absentTaxon) {
        STITree copyMainTree = new STITree(mainTree);
        String mainTreeNewick = mainTree.toNewick();
        System.out.println("Converted to Str: " + mainTreeNewick);

        updateLeaves.remove(absentTaxon);
        Iterable<String> iterable = updateLeaves;
        copyMainTree.constrainByLeaves(iterable);
        System.out.println("After constraint: " + mainTreeNewick);

        String searchStr = copyMainTree.toNewick();
        searchStr = searchStr.substring(0, searchStr.indexOf(";"));
        System.out.println("searchStr: " + searchStr);

        String replaceStr = updateSubtree.toNewick();
        replaceStr = replaceStr.substring(0, replaceStr.indexOf(";"));
        System.out.println("replaceStr: " + replaceStr);

        String prevStr = mainTreeNewick;
        mainTreeNewick = mainTreeNewick.replaceAll(Pattern.quote(searchStr), replaceStr);

        if(prevStr.equals(mainTreeNewick)) System.out.println("Error: "+searchStr+" Not found; check for discordance");

        System.out.println("final newick: " + mainTreeNewick);

        return mainTreeNewick;
    }

    public static String addSubtreesWithMissingTaxa(STITree stiTree, STITree t1) {
        List<String> taxa_list = new ArrayList<>(Arrays.asList(new String[]{"1", "4", "10", "5", "2", "8", "6", "7", "11", "3", "9"}));

        List<String> absentTaxa = new ArrayList<>();
        for (String taxon : taxa_list) {
            if (!Arrays.asList(stiTree.getLeaves()).contains(taxon)) absentTaxa.add(taxon);
        }
        System.out.println(absentTaxa);
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
                    String updatedStiTree = updateMainTree(stiTree, thisNode,thisNodeLeaves, taxon);
                    try {
                        stiTree = new STITree(updatedStiTree);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    System.out.println(thisNode);
                    targetNode.add(thisNode);
                    found++;
                }
            }
            if (found == absentTaxa.size()) break;
        }
        return stiTree.toNewick();

    }

    public static TreeTable run(TreeTable treeTable, TreeTable t1) {
        System.out.println("===============================================================");
        System.out.println(treeTable);
        if (treeTable==null) return t1;
        STITree newickTree1 = null;
        STITree newickTree2 = null;
        try {
            newickTree1 = new STITree(treeTable.getTree());
            newickTree2 = new STITree(t1.getTree());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        // String srchStr ="((5,6),(9,(8,7)))";
        // String repStr = "(10,((5,6),(9,(8,7))))";

        String updatedStr = addSubtreesWithMissingTaxa(newickTree1, newickTree2);
        System.out.println(updatedStr);
        //check: The first node found with any absent taxa should contain all absent taxa
        // for(String taxon: absentTaxa){
        //     if(!Arrays.asList(targetNode.getLeaves()).contains(taxon)) System.out.println("Error!!!! taxon: "+taxon+" not present in target node");
        // }
        treeTable.setTree(updatedStr);
        treeTable.setTag(treeTable.getTag() + "-" + t1.getTag());
        treeTable.setSupport(treeTable.getSupport() + t1.getSupport());

        return treeTable;

    }
}
