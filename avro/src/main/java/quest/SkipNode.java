package quest;
import codec.metadata.FileColumnMetaData;
import codec.metadata.misc.ValueType;
import columnar.BlockColumnValues;


public class SkipNode {

    int nodeId;
    FileColumnMetaData metadata;

    String columnName;

    BlockColumnValues value;
    int skipHeight;

    SkipNode[] skipAncestors;

    int leftBound = -1;
    int rightBound = -1;

    int[] counter;

    SkipNode parentNode = null;

    public SkipNode(int nodeId, FileColumnMetaData metadata, BlockColumnValues value){
        this.nodeId = nodeId;
        this.metadata = metadata;
        this.value = value;
        this.columnName = nodeId == -1 ? "" :metadata.getName();
//        System.out.println(this.columnName);
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public boolean isArray(){
        return this.value.isArray();
    }

    public int getNodeId(){
        return this.nodeId;
    }

    public BlockColumnValues getValue(){
        return this.value;
    }

    public SkipNode[] getSkipAncestors(){
        return this.skipAncestors;
    }

    public SkipNode getSkipAncestor(int index){
        return this.skipAncestors[index];
    }



    public ValueType getType(){
        return this.metadata.getType();
    }

    public int getSkipHeight() {
        return skipHeight;
    }

    public void setSkipHeight(int skipHeight) {
        this.skipHeight = skipHeight;
    }

    /**
     * 原来metadata的层数无父节点情况下，layer从0开始排
     * 修改为从1开始排，虚拟父节点层数为0
     * @return 返回实例的层数
     */
    public int getNodeLayer(){
        if(this.nodeId == -1)return 0;
        return metadata.getLayer() + 1;
    }

    public int getLeftBound() {
        return leftBound;
    }

    public void setLeftBound(int leftBound) {
        this.leftBound = leftBound;
    }

    public int getRightBound() {
        return rightBound;
    }


    public void setRightBound(int rightBound) {
        this.rightBound = rightBound;
    }

    /**
     * 判断当前节点是否为给定node节点的祖先节点
     * @param node 给定的node节点
     * @return 如果当前实例是node的祖先，返回true，否则返回false
     */
    public boolean isAncestor(SkipNode node){
        if(this.nodeId == -1){
            return true;
        }
        return leftBound <= node.getNodeId() && rightBound >= node.getNodeId();
    }

    public void setSkipAncestors(SkipNode[] skipAncestors){
        this.skipAncestors = skipAncestors;
    }

    public SkipNode getParentNode() {
        return this.parentNode;
    }

    public void setParentNode(SkipNode parentNode) {
        this.parentNode = parentNode;
    }

    public FileColumnMetaData getMetaData(){
        return this.metadata;
    }

    public String skipAncestorsId() {
        StringBuilder idList = new StringBuilder("[");
        for (SkipNode m : this.skipAncestors){
            if(m == null){
                idList.append("null, ");
            }else{
                idList.append(m.getNodeId()).append(", ");
            }

        }
        idList.append("]");
        return idList.toString();
    }

    @Override
    public String toString() {
        return "SkipNode{" +
                "nodeId=" + nodeId +
                ", metadata=" + metadata +
                "nodeLayer=" + getNodeLayer() +
                ", skipHeight=" + skipHeight +
                ", skipAncestors=" + skipAncestorsId() +
                ", parentNode=" + (parentNode == null ? -1 : parentNode.getNodeId()) +
                '}';
    }
}
