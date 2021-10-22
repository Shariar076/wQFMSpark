package utils;

import legacy.configs.Config;
import legacy.configs.DefaultValues;
import phylonet.tree.io.ParseException;
import phylonet.tree.model.sti.STITree;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class TreeHandler {
    public static String rerootTree(String newickTree, String outGroupNode) {
        switch(Config.REROOT_MODE) {
            case DefaultValues.REROOT_USING_JAR:
                return TreeHandler.rerootTree_JAR(newickTree, outGroupNode);
            case DefaultValues.REROOT_USING_PYTHON:
                return null; //TreeHandler.rerootTree_python(newickTree, outGroupNode);
            case DefaultValues.REROOT_USING_PERL:
                return null; //TreeHandler.rerootTree_Perl(newickTree, outGroupNode);
            default:
                System.out.println("-->>FOR NOW Reroot only support using jar and python dendropy [to add perl later].");
                return "NULL";
        }
    }
    private static String rerootTree_JAR(String newickTree, String outGroupNode) {
//        String newickTree = "((3,(1,2)),((6,5),4));";
//        String outGroupNode = "5";
        STITree tree = null;

        try {
            tree = new STITree(newickTree);
            tree.rerootTreeAtNode(tree.getNode(outGroupNode));
        } catch (IOException | ParseException ex) {
            System.out.println("Error in rerootTree.JAR ... check if jar main.jar exists. Exiting.");
            System.exit(-1);
        }

        return tree.toNewick();
    }
    public static String getStarTree(List<Integer> taxa_list_int) {
        if (taxa_list_int.isEmpty()) {
            return "();";
        }
        return taxa_list_int
                .stream()
                .map(String::valueOf)
                .collect(Collectors.joining(",", "(", ")"));
    }

    private static String addBracketsAndSemiColon(String s) {
        return "(" + s + ");";
    }

    private static String removeOutgroupNodeAndBrackets(String tree, String outGroup) {
        tree = tree.replace(";", ""); // remove semi-colon
        tree = tree.substring(1, tree.length() - 1); // remove first and last brackets
        tree = tree.replace(outGroup, ""); //remove outGroup Node
        tree = tree.substring(1); //From left, so remove first comma
        return tree;
    }

    private static String mergeTwoRootedTrees(String treeLeft, String treeRight, String outGroup) {

        String leftTree_outgroupRemoved = removeOutgroupNodeAndBrackets(treeLeft, outGroup);
        String rightTree_outgroupRemoved = removeOutgroupNodeAndBrackets(treeRight, outGroup); //CHECK if from both sides outgroup are in left

        String mergedTree = addBracketsAndSemiColon(leftTree_outgroupRemoved + "," + rightTree_outgroupRemoved);

        return mergedTree;
    }

    public static String mergeUnrootedTrees(String treeLeft, String treeRight, String outGroup) {
        //Check these two conditions
        if (treeLeft.equals("")) {
            return treeRight;
        }
        if (treeRight.equals("")) {
            return treeLeft;
        }

        //1. reroot two trees wrt outGroup
        String rootedTreeLeft = TreeHandler.rerootTree_JAR(treeLeft, outGroup);
        String rootedTreeRight = TreeHandler.rerootTree_JAR(treeRight, outGroup);
        //2. Outgroup will be at the left-most side, so we have to right-shift the leftTree's outgroup
//        String rootedTreeLeft_rightShifted = TreeHandler.shiftOutgroupToRight(rootedTreeLeft);
        String mergedTree = mergeTwoRootedTrees(rootedTreeLeft, rootedTreeRight, outGroup);
        return mergedTree;
    }
}
