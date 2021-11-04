package structure;

import phylonet.tree.io.ParseException;
import phylonet.tree.model.sti.STITree;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class SerializedSTITree implements Serializable {
    STITree tree;

    public SerializedSTITree(String newickTree) throws IOException, ParseException {
        this.tree = new STITree(newickTree);
    }

    public SerializedSTITree(SerializedSTITree tree) {
        this.tree=tree.tree;
    }

    public void constrainByLeaves(Iterable<String> iterable){
        this.tree.constrainByLeaves(iterable);
    }

    public String[] getLeaves(){
        return this.tree.getLeaves();
    }

    public SerializedSTINode getNode(int id){
        return new SerializedSTINode(this.tree.getNode(id));
    }

    public String toNewick(){
        return this.tree.toNewick();
    }

    public int getNodeCount() {
        return this.tree.getNodeCount();
    }
}
