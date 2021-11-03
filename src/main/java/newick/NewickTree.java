package newick;

import phylonet.tree.io.ParseException;
import phylonet.tree.model.sti.STITree;

import java.io.IOException;
import java.util.*;

public class NewickTree {
    private static int nodeId = 0;

    static class Node implements Comparable<Node>{
        public String name;
        public boolean isNamed;
        public ArrayList<Node> children;
        public int weight;

        Node(String name) {
            int colonIndex = name.indexOf(':');
            String actualNameText;
            children = new ArrayList<>();
            if (colonIndex == -1) {
                actualNameText = name;
                weight = 0;
            } else {
                actualNameText = name.substring(0, colonIndex);
                weight = Integer.parseInt(name.substring(colonIndex + 1));
            }

            if (actualNameText.equals("")) {
                this.isNamed = false;
                this.name = Integer.toString(nodeId);
                nodeId++;
            } else {
                this.isNamed = true;
                this.name = actualNameText;
            }
        }

        String getName() {
            if (isNamed)
                return name;
            else
                return "";
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (children != null && children.size() > 0) {
                sb.append("(");
                for(Node child: children){
                    sb.append(child.toString());
                    sb.append(",");
                }
                // for (int i = 0; i < children.size() - 1; i++) {
                //     sb.append(children.get(i).toString());
                //     sb.append(",");
                // }
                // sb.append(children.get(children.size() - 1).toString());
                sb = new StringBuilder(sb.substring(0, sb.length() - 1));
                sb.append(")");
            }
            if (name != null) sb.append(this.getName());
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Node)) return false;
            Node node = (Node) o;
            Collections.sort(children);
            Collections.sort(node.children);
            if (isNamed && node.isNamed) return weight == node.weight
                    && Objects.equals(name, node.name);
                    // && Objects.equals(children, node.children);

            else if (!isNamed && !node.isNamed) return weight == node.weight;
                    // && Objects.equals(children, node.children);
            else return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name,
                    // children,
                    weight);
        }

        @Override
        public int compareTo(Node node) {
            return String.CASE_INSENSITIVE_ORDER.compare(name, node.name);
        }
    }

    public Node root;
    public ArrayList<Node> nodeList = new ArrayList<>();

    static NewickTree readNewickFormat(String newick) {
        return new NewickTree(newick);
    }

    private NewickTree(String newick) {

        // single branch = subtree (?)
        this.root = readSubtree(newick.substring(0, newick.length() - 1)); // leave the semicolon

    }

    @Override
    public String toString() {
        return root.toString() + ";";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NewickTree)) return false;
        NewickTree that = (NewickTree) o;
        Collections.sort(nodeList);
        Collections.sort(that.nodeList);
        return Objects.equals(root, that.root)
                && Objects.equals(nodeList, that.nodeList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(root);
    }

    private static String[] split(String s) {

        ArrayList<Integer> splitIndices = new ArrayList<>();

        int rightParenCount = 0;
        int leftParenCount = 0;
        for (int i = 0; i < s.length(); i++) {
            switch (s.charAt(i)) {
                case '(':
                    leftParenCount++;
                    break;
                case ')':
                    rightParenCount++;
                    break;
                case ',':
                    if (leftParenCount == rightParenCount) splitIndices.add(i);
                    break;
            }
        }

        int numSplits = splitIndices.size() + 1;
        String[] splits = new String[numSplits];

        if (numSplits == 1) {
            splits[0] = s;
        } else {

            splits[0] = s.substring(0, splitIndices.get(0));

            for (int i = 1; i < splitIndices.size(); i++) {
                splits[i] = s.substring(splitIndices.get(i - 1) + 1, splitIndices.get(i));
            }

            splits[numSplits - 1] = s.substring(splitIndices.get(splitIndices.size() - 1) + 1);
        }

        return splits;
    }

    private Node readSubtree(String s) {

        int leftParen = s.indexOf('(');
        int rightParen = s.lastIndexOf(')');

        if (leftParen != -1 && rightParen != -1) {

            String name = s.substring(rightParen + 1);
            String[] childrenString = split(s.substring(leftParen + 1, rightParen));

            Node node = new Node(name);
            // node.children = new ArrayList<>();
            for (String sub : childrenString) {
                Node child = readSubtree(sub);
                node.children.add(child);
            }

            nodeList.add(node);
            return node;
        } else if (leftParen == rightParen) {

            Node node = new Node(s);
            nodeList.add(node);
            return node;

        } else throw new RuntimeException("unbalanced ()'s");
    }

    public static void main(String[] args) {
        // String newickStr1 = "(((10,8,9,7),(6,5)),(4,(3,(1,2))));";
        // String newickStr2 = "(((10,8,9,7),(6,5)),(4,(3,(1,2))));";
        // String newickStr3 = "((4,(3,(1,2))),((10,8,9,7),(6,5)));";
        //
        // NewickTree sampleNewickTree1 = NewickTree.readNewickFormat(newickStr1);
        // NewickTree sampleNewickTree2 = NewickTree.readNewickFormat(newickStr2);
        // NewickTree sampleNewickTree3 = NewickTree.readNewickFormat(newickStr3);
        //
        // System.out.println(sampleNewickTree1);
        // System.out.println(sampleNewickTree2);
        // System.out.println(sampleNewickTree3);
        //
        // System.out.println(sampleNewickTree1.equals(sampleNewickTree2));
        // System.out.println(sampleNewickTree1.equals(sampleNewickTree3));
        //
        // for (Node node : sampleNewickTree1.nodeList) {
        //     System.out.println(node.toString());
        // }
        String annotatedString = "((10,((5,6)4.305555555555555,(9,(8,7)4.583333333333333)4.023809523809524)4.333333333333333)5.0,(11,((3,4)3.75,(1,2)3.9444444444444446)5.0)5.0);";
        try {
            STITree tree =new STITree(annotatedString);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}
