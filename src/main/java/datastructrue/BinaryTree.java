package datastructrue;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class BinaryTree<K extends Comparable<K>,V> {

    private TreeNode<K,V> root;

    //新增节点
    public void put(K key,V value){
        TreeNode<K, V> node = new TreeNode<>(key, value);
        if(null==root)
            this.root= node;
        else {
            TreeNode<K,V> parent=this.root;
            boolean looping=true;
            boolean addLeft=false;
            while (looping){
                if(node.getKey().compareTo(parent.getKey())<0){
                    if(null==parent.getLeftChild()){
                        looping=false;
                        addLeft=true;
                    }
                    else
                        parent=parent.getLeftChild();
                }
                else {
                    if(null==parent.getRightChild()){
                        looping=false;
                        addLeft=false;
                    }
                    else
                        parent=parent.getRightChild();
                }
            }

            if(addLeft)
                parent.setLeftChild(node);
            else
                parent.setRightChild(node);
            node.setParent(parent);
        }
    }

    //删除节点
    public void remove(K key) throws Exception {
        TreeNode<K, V> node = search(key);
        if(null==node)
            throw new Exception(String.format("%s is not found in tree",key));

        while (null!=node){
            boolean isLeaf = isLeafNode(node);

            //叶子节点时直接删除
            if(isLeaf){
                boolean isLeft = node.getParent().getLeftChild() == node;
                if(isLeft)
                    node.getParent().setLeftChild(null);
                else
                    node.getParent().setRightChild(null);
                node=null;
            }

            else {
                //非叶子节点时用后继节点替换
                TreeNode<K, V> successor = successorOf(node);
                node.setValue(successor.getValue());
                node=successor;
            }
        }
    }


    //查找节点
    protected TreeNode<K,V> search(K key){
        TreeNode<K, V> node = this.root;
        while (null!=node){
            if(node.getKey().equals(key))
                return node;
            if(key.compareTo(node.getKey())<=0)
                node=node.getLeftChild();
            else
                node=node.getRightChild();
        }
        return null;
    }

    /**  x0                              x0
     *   |                               |
     *   x1                              x3
     *  /  \      对x1进行左旋          /  \
     * x2  x3    --------------->      x1  x5
     *    /  \                        /  \
     *    x4  x5                     x2  x4
     */
    protected void leftRotate(TreeNode<K,V> node) throws Exception {
        if(null==node.getRightChild())
            throw new Exception("node doesn't have right child node, can't do left rotate");

        TreeNode<K, V> x0 = node.getParent();
        TreeNode<K, V> x1 = node;
        TreeNode<K, V> x3 = x1.getRightChild();
        TreeNode<K, V> x4 = x3.getLeftChild();

        //各节点的变更
        //x0
        if(x1==x0.getLeftChild())
            x0.setLeftChild(x3);
        else
            x0.setRightChild(x3);

        //x1
        x1.setParent(x3);
        x1.setRightChild(x4);

        //x3
        x3.setParent(x0);
        x3.setLeftChild(x1);

        //x4
        x4.setParent(x1);
    }

    /**     x0                          x0
     *      |                           |
     *      x1                          x2
     *     /  \     对x1进行右旋       /  \
     *    x2  x3   --------------->   x4  x1
     *   /  \                            /  \
     *  x4  x5                          x5  x3
     */
    protected void rightRotate(TreeNode<K,V> node) throws Exception {
        if(null==node.getLeftChild())
            throw new Exception("node doesn't have left child node, can't do right rotate");

        TreeNode<K, V> x0 = node.getParent();
        TreeNode<K, V> x1 = node;
        TreeNode<K, V> x2 = x1.getLeftChild();
        TreeNode<K, V> x5 = x2.getRightChild();

        //各节点的变更
        //x0
        if(x1==x0.getLeftChild())
            x0.setLeftChild(x2);
        else
            x0.setRightChild(x2);

        //x1
        x1.setParent(x2);
        x1.setLeftChild(x5);

        //x2
        x2.setParent(x0);
        x2.setRightChild(x1);

        //x5
        x5.setParent(x1);
    }



    //获取前驱节点
    protected TreeNode<K,V> predecessorOf(TreeNode<K,V> node){
        boolean isLeaf = isLeafNode(node);
        if(!isLeaf)
            return maxNodeOf(node.getLeftChild());
        else {
            TreeNode<K,V> child=node;
            TreeNode<K,V> parent=node.getParent();
            while (parent.getRightChild()!=child){
                child=parent;
                parent=parent.getParent();
            }
            return parent;
        }
    }

    //获取后继节点
    protected TreeNode<K,V> successorOf(TreeNode<K,V> node){
        boolean isLeaf = isLeafNode(node);
        if(!isLeaf)
            return minNodeOf(node.getRightChild());
        else {
            TreeNode<K,V> child=node;
            TreeNode<K,V> parent=node.getParent();
            while (parent.getLeftChild()!=child){
                child=parent;
                parent=parent.getParent();
            }
            return parent;
        }
    }

    protected boolean isLeafNode(TreeNode<K,V> node){
        return null==node.getLeftChild() && null==node.getRightChild();
    }

    //获取root子树的最大节点
    protected TreeNode<K,V> maxNodeOf(TreeNode<K,V> root){
        if(null==root)
            return null;
        TreeNode<K, V> parent = root;
        while (null!=parent.getRightChild())
            parent=parent.getRightChild();
        return parent;
    }

    //获取root子树的最小节点
    protected TreeNode<K,V> minNodeOf(TreeNode<K,V> root){
        if(null==root)
            return null;
        TreeNode<K, V> parent = root;
        while (null!=parent.getLeftChild())
            parent=parent.getLeftChild();
        return parent;
    }

    //中序遍历二叉树
    public void iterateTree(){
        this.iterateTree(this.root);
        System.out.println();
    }

    private void iterateTree(TreeNode<K,V> node){
        if(null==node)
            return;
        iterateTree(node.getLeftChild());
        System.out.print(node);
        System.out.print("\t");
        iterateTree(node.getRightChild());
    }

    //打印二叉树
    public void show(){

        int leftDepth=0;
        TreeNode<K, V> child = this.root;
        while (null!=child){
            leftDepth+=1;
            child=child.getLeftChild();
        }

        ArrayList<TreeNode<K, V>> treeNodes = new ArrayList<>();
        treeNodes.add(this.root);
        HashMap<String, Double> offsetMap = new HashMap<>();
        ArrayList<ArrayList<Tuple2<Double,String>>> offsets = new ArrayList<>();

        while (treeNodes.size()>0){
            ArrayList<Tuple2<Double, String>> nodeOffsets = new ArrayList<>();
            ArrayList<Tuple2<Double, String>> branchOffsets = new ArrayList<>();

            int size = treeNodes.size();
            for (int i = 0; i < size;i++) {
                TreeNode<K, V> node = treeNodes.get(0);
                treeNodes.remove(node);

                if(node==this.root)
                    offsetMap.put(node.getKey().toString(),Math.pow(2,leftDepth));
                else
                {
                    if(node==node.getParent().getLeftChild())
                        offsetMap.put(node.getKey().toString(),offsetMap.get(node.getParent().getKey().toString()) - 2);
                    else
                        offsetMap.put(node.getKey().toString(),offsetMap.get(node.getParent().getKey().toString()) + 2);
                }


                Tuple2<Double, String> offsetTuple = new Tuple2<>();
                offsetTuple.f1=node.getKey().toString();
                offsetTuple.f0=offsetMap.get(offsetTuple.f1);
                nodeOffsets.add(offsetTuple);

                if (null!=node.getLeftChild()){
                    treeNodes.add(node.getLeftChild());
                    branchOffsets.add(new Tuple2<>(offsetTuple.f0, "/"));
                }
                if(null!=node.getRightChild()){
                    treeNodes.add(node.getRightChild());
                    branchOffsets.add(new Tuple2<>(offsetTuple.f0, "\\"));
                }
            }

            offsets.add(nodeOffsets);
            offsets.add(branchOffsets);
        }

        String treeStr = offsets.stream().map(layerOffset -> {
            StringBuilder sb = new StringBuilder();
            double indent = 0;
            for (int i = 0; i < layerOffset.size(); i++) {
                sb.append(StringUtil.generateSpace(Math.max(layerOffset.get(i).f0 - indent,1))).append(layerOffset.get(i).f1);
                indent = layerOffset.get(i).f0 + indent;
            }
            return sb.toString();
        }).collect(Collectors.joining("\n"));

        System.out.println(treeStr);
    }
}

class TreeNode<K,V> {

    private K key;
    private V value;
    private TreeNode<K,V> parent;
    private TreeNode<K,V> leftChild;
    private TreeNode<K,V> rightChild;

    public TreeNode(){}

    public TreeNode(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public TreeNode<K,V> getParent() {
        return parent;
    }

    public void setParent(TreeNode<K,V> parent) {
        this.parent = parent;
    }

    public TreeNode<K,V> getLeftChild() {
        return leftChild;
    }

    public void setLeftChild(TreeNode<K,V> leftChild) {
        this.leftChild = leftChild;
    }

    public TreeNode<K,V> getRightChild() {
        return rightChild;
    }

    public void setRightChild(TreeNode<K,V> rightChild) {
        this.rightChild = rightChild;
    }

    @Override
    public String toString() {
        return "TreeNode{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
