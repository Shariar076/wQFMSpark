package structure;

import phylonet.tree.io.ParseException;
import phylonet.tree.model.TNode;
import phylonet.tree.model.sti.STINode;
import phylonet.tree.model.sti.STITree;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

public class SerializedSTINode implements Serializable {
    STINode node;

    public SerializedSTINode(STINode node) {
        this.node = node;
    }

    public SerializedSTINode(Object node) {
        this.node = (STINode) node;
    }
    public String toString(){
        return this.node.toString();
    }

    public String toNewick(){
        return this.node.toNewick();
    }

    public Iterable<SerializedSTINode> getLeaves() {
        ArrayList<SerializedSTINode> leaves =new ArrayList<>();
        for(Object node: this.node.getLeaves()){
            leaves.add(new SerializedSTINode(node));
        }
        return leaves;
    }
}
