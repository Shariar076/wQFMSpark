package structure;

import java.io.Serializable;

public class TreeTable implements Serializable {
    public String tree;
    public String tag;
    public double support;

    public TreeTable() {
    }

    public String getTree() {
        return tree;
    }

    public void setTree(String tree) {
        this.tree = tree;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public double getSupport() {
        return support;
    }

    public void setSupport(double support) {
        this.support = support;
    }

    @Override
    public String toString() {
        return "TreeTable{" +
                "tree='" + tree + '\'' +
                ",\n tag='" + tag + '\'' +
                ",\n support=" + support +
                '}';
    }
}
