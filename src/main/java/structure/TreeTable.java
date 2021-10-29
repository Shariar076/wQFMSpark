package structure;

import java.util.ArrayList;

public class TreeTable {
    public String tree;
    public String tag;
    public int support;

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

    public int getSupport() {
        return support;
    }

    public void setSupport(int support) {
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
