package quest;

import codec.metadata.FileColumnMetaData;
import columnar.BlockColumnValues;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;

public class SkipTree {
    SkipNode[] skipNodes;

    SkipNode root;
    int nodeNumber;
    int maxLayer;
    int maxTableHeight;
    int[] skipTableHeightList;

    boolean genMeta = false;

    String dir;

//    HashMap<Integer, BitSet> lcaBitsetMap = new HashMap<>();
    int freeMemory = 4; //读文件的可用内存大小

    //预存每一个文件的bufferedReader，重新创建需要时间，直接保存在内存中
    HashMap<String, FileChannel> indexToReader = new HashMap<>();

    HashSet<Integer> vertex;// 记录schema中所有的顶点，不包含根节点

    HashSet<Integer> columnIndex; //记录schema中所有的column index列
    HashMap<Integer, Integer> vertexToColumnIndex; // 实体顶点节点对应的同一层中的column index
    HashMap<Integer, Integer> columnIndexToVertex; // 每个column index节点对应的实体节点

    public SkipTree(FileColumnMetaData[] metadata, BlockColumnValues[] values, String dir) throws IOException {
        this.dir = dir;
        this.nodeNumber = metadata.length;
        this.skipTableHeightList = new int[this.nodeNumber];
        this.skipNodes = new SkipNode[this.nodeNumber];
        this.root = new SkipNode(-1, null, null);
        //将metadata格式转为skipNode格式
        HashMap<FileColumnMetaData, Integer> map = new HashMap<>();
        for(int i = 0; i < this.nodeNumber; i++){
            this.skipNodes[i] = new SkipNode(i, metadata[i], values[i]);
            if(metadata[i].isArray()){
                //如果该列为数组类型则设置子节点的左/右边界
                this.skipNodes[i].setLeftBound(i);
                for(int j = i + 1; j < this.nodeNumber; j++){
                    if (metadata[j].getLayer() == metadata[i].getLayer()){
                        this.skipNodes[i].setRightBound(j - 1);
                    }
                }
                if (this.skipNodes[i].getRightBound() == -1){
                    this.skipNodes[i].setRightBound(this.nodeNumber);
                }
            }
            map.put(metadata[i], i);
        }
        //维护skipNode的ParentNode
        for(int i = 0; i < this.nodeNumber; i++){

            if (this.skipNodes[i].getNodeLayer() == 1){
                //第一层节点设置虚拟父节点root
                skipNodes[i].setParentNode(root);
            }else{
                this.skipNodes[i].setParentNode(this.skipNodes[map.get(metadata[i].getParent())]);
            }
        }
        this.maxLayer = getMaxLayer();
        this.maxTableHeight = (int)(Math.log(this.maxLayer) / Math.log(2.0));
        map.clear();
        //初始化每个节点在跳表中的高度
        InitialHeight();

        //设置每个节点的跳表祖先
        maintainSkipAncestors();

    }

    public SkipTree(FileColumnMetaData[] metadata, BlockColumnValues[] values, HashSet<Integer> vertex, HashSet<Integer> columnIndex,
                    HashMap<Integer, Integer> vertexToColumnIndex, HashMap<Integer, Integer> columnIndexToVertex, String dir, boolean genMeta) throws IOException{
        this.dir = dir;
        this.genMeta = genMeta;
        this.nodeNumber = metadata.length;
        this.skipTableHeightList = new int[this.nodeNumber];
        this.skipNodes = new SkipNode[this.nodeNumber];
        this.vertex = vertex;
        this.columnIndex = columnIndex;
        this.vertexToColumnIndex = vertexToColumnIndex;
        this.columnIndexToVertex = columnIndexToVertex;
        this.root = new SkipNode(-1, null, null);
        //将metadata格式转为skipNode格式
        HashMap<FileColumnMetaData, Integer> map = new HashMap<>();
        for(int i = 0; i < this.nodeNumber; i++){
            this.skipNodes[i] = new SkipNode(i, metadata[i], values[i]);
            if(metadata[i].isArray()){
                //如果该列为数组类型则设置子节点的左/右边界
                this.skipNodes[i].setLeftBound(i);
                for(int j = i + 1; j < this.nodeNumber; j++){
                    if (metadata[j].getLayer() == metadata[i].getLayer()){
                        this.skipNodes[i].setRightBound(j - 1);
                    }
                }
                if (this.skipNodes[i].getRightBound() == -1){
                    this.skipNodes[i].setRightBound(this.nodeNumber);
                }
            }
            map.put(metadata[i], i);
        }
        //维护skipNode的ParentNode
        for(int i = 0; i < this.nodeNumber; i++){

            if (this.skipNodes[i].getNodeLayer() == 1){
                //第一层节点设置虚拟父节点root
                skipNodes[i].setParentNode(root);
            }else{
                this.skipNodes[i].setParentNode(this.skipNodes[map.get(metadata[i].getParent())]);
            }
        }
        this.maxLayer = getMaxLayer();
        this.maxTableHeight = (int)(Math.log(this.maxLayer) / Math.log(2.0));
        map.clear();
        InitialHeight();

        maintainSkipAncestorsInGraph();
    }

    /**
     * 查找schema的最大层数
     * @return 返回schema的最大层数
     */
    public int getMaxLayer(){
        int maxLayer = 0;
        for (SkipNode m : skipNodes){
            maxLayer = Math.max(maxLayer, m.getNodeLayer());
        }
        return maxLayer;
    }

    /**
     * 初始化每个skipNode在跳表中的高度
     */
    public void InitialHeight(){
        this.root.setSkipHeight(this.maxTableHeight);
        for (int i = 0; i < this.nodeNumber; i++) {
            int h = 0;
            for(int j = 1; j <= this.maxTableHeight; j++){
                if(this.skipNodes[i].getNodeLayer() % Math.pow(2, j) == 0){
                    h++;
                }
            }
            this.skipNodes[i].setSkipHeight(h);
            this.skipTableHeightList[i] = h;
        }
    }


    /**
     * 设置每个节点对应的跳表祖先。每个节点跳树祖先列表有k个祖先，k为跳树的最大层数，每层维护层数相同的第一个祖先以及预计算的counters。
     */
    public void maintainSkipAncestors() throws IOException {
        for (int i = 0; i < this.nodeNumber; i++){
            int ancestorNum = this.skipNodes[i].getSkipHeight() + 1;
            SkipNode[] skipAncestors = new SkipNode[ancestorNum];
            skipAncestors[0] = this.skipNodes[i].getParentNode();
            computeCounter(i, skipAncestors[0].getNodeId());
            for (int j = 1; j < ancestorNum; j++){
                SkipNode nowNode = this.skipNodes[i];
                while (nowNode.getParentNode() != null && nowNode.getParentNode().getSkipHeight() < j){
                    nowNode = nowNode.getParentNode();
                }
                if (nowNode.getParentNode() == null){
                    skipAncestors[j] = root;
                }
                else{
                    skipAncestors[j] = nowNode.getParentNode();
                    computeCounter(i, nowNode.getParentNode().getNodeId());
                }
            }
            this.skipNodes[i].setSkipAncestors(skipAncestors);
        }

    }

    public void maintainSkipAncestorsInGraph() throws IOException {
        for (int i = 0; i < this.nodeNumber; i++){
            int ancestorNum = this.skipNodes[i].getSkipHeight() + 1;
            SkipNode[] skipAncestors = new SkipNode[ancestorNum];
            skipAncestors[0] = this.skipNodes[i].getParentNode();
            if (genMeta){
                computeCounter(i, skipAncestors[0].getNodeId());
            }

            for (int j = 1; j < ancestorNum; j++){
                SkipNode nowNode = this.skipNodes[i];
                while (nowNode.getParentNode() != null && nowNode.getParentNode().getSkipHeight() < j){
                    nowNode = nowNode.getParentNode();
                }
                if (nowNode.getParentNode() == null){
                    skipAncestors[j] = root;
                }
                else{
                    skipAncestors[j] = nowNode.getParentNode();
                    if (genMeta){
                        computeCounterInGraph(i, nowNode.getParentNode().getNodeId());
                    }

                }
            }
            this.skipNodes[i].setSkipAncestors(skipAncestors);
        }
    }

    /**
     * @description 计算当前两个节点的合并counter并且写入外存文件中。
     * @param nowNodeId 子节点id
     * @param anceNodeId skip 祖先节点的id
     * @throws IOException ...
     */
    public void computeCounter(int nowNodeId, int anceNodeId) throws IOException {
//        String dir = "T:/cores/avro/src/test/resources/metadata/" + nowNodeId + "_" + anceNodeId+".bin";
        String nowDir = dir +nowNodeId + "_" + anceNodeId + ".bin";
        int n1Index = nowNodeId;
        BlockColumnValues nowValue = this.skipNodes[nowNodeId].getValue();
        int lastRow = nowValue.getLastRow();
        int[] counter = new int[lastRow];
        if (!this.skipNodes[nowNodeId].isArray()){
            //如果当前节点是一个基本节点（叶节点）
            for (int i = 0; i < lastRow; i++){
                counter[i] = i + 1;
            }
        }
        else{
            //当前节点是一个非叶节点，本身存在counter
            int i = 0;
            nowValue.create();
            while(nowValue.hasNext()){
                nowValue.startRow();
                counter[i++] = nowValue.nextLength();
            }
        }
        int[] ansCounter;
        while(this.skipNodes[nowNodeId].getParentNode().getNodeId() != anceNodeId){
            nowNodeId = this.skipNodes[nowNodeId].getParentNode().getNodeId();
            BlockColumnValues nextValue = this.skipNodes[nowNodeId].getValue();
            ansCounter = new int[nextValue.getLastRow()];
            nextValue.create();
            int i = 0;
            while(nextValue.hasNext()){
                nextValue.startRow();
                int nextNum = nextValue.nextLength();
                if(nextNum == 0){
                    ansCounter[i++] = 0;
                    continue;
                }
                ansCounter[i++] = counter[nextNum - 1];
            }
            counter = ansCounter;
        }
        File counterFile = new File(nowDir);

//        ByteBuffer bbf = ByteBuffer.allocate(4 * counter.length); // 采用全局index方式存储


        ByteBuffer bbf = ByteBuffer.allocate(2 * counter.length);

        FileChannel nowFileChannel = new FileOutputStream(counterFile, false).getChannel();


//        FileWriter counterWriter = new FileWriter(new File("E:/WorkSpace/cores-master/avro/src/test/resources/five_layer/metadata/"
//                + n1Index + "__" + anceNodeId+".txt"));
//        for (int i = 0; i < counter.length; i++){
//            counterWriter.write(String.valueOf(counter[i]));
//            if(i != counter.length - 1){
//                counterWriter.write("\n");
//            }
//        }
//        counterWriter.flush();
//        counterWriter.close();


        //低字节序写入文件
        int lastCounter = 0;
        for (int i : counter) {
            bbf.put((byte) ((i - lastCounter) & 0xff));
            bbf.put((byte) ((i - lastCounter) >> 8 & 0xff));
            lastCounter = i;
        }



        // 采用全局index方式存储
//        for (int j : counter) {
//            bbf.put((byte) (j & 0xff));
//            bbf.put((byte) (j >> 8 & 0xff));
//            bbf.put((byte) (j >> 16 & 0xff));
//            bbf.put((byte) (j >> 24 & 0xff));
//        }
//        System.out.println(bbf);


        bbf.flip();
        nowFileChannel.write(ByteBuffer.wrap(bbf.array()));
        nowFileChannel.close();
        FileInputStream fis = new FileInputStream(nowDir);

        // 1.从FileInputStream对象获取文件通道FileChannel
        FileChannel channel = fis.getChannel();
//        BufferedReader br = new BufferedReader(new FileReader(dir));
//        br.mark((int)counterFile.length()+1);
        this.indexToReader.put(n1Index + "_" + anceNodeId, channel);
    }

    /**
     * @description 在图数据中处理预计算
     * @param nowNodeId 叶子节点
     * @param anceNodeId 叶子节点对应的祖先节点
     * @throws IOException ...
     */
    public void computeCounterInGraph(int nowNodeId, int anceNodeId) throws IOException {
//        String dir = "T:/cores/avro/src/test/resources/ldbc_new/metadata/" + nowNodeId + "_" + anceNodeId + ".txt";
        String nowDir = dir + nowNodeId + "_" + anceNodeId + ".txt";
//        int n1Index = nowNodeId;
        BlockColumnValues nowValue = this.skipNodes[nowNodeId].getValue();
        int lastRow = nowValue.getLastRow();
//        int[] counter = new int[lastRow];
        if(this.vertex.contains(nowNodeId)) {
            //如果nowNode为顶点节点，则预计算需要处理两顶点之间的跳跃

            //1. 读出该节点与上一届点的column index 和 counter
            int colidx = this.vertexToColumnIndex.get(nowNodeId);
            SkipNode nowNode = this.skipNodes[nowNodeId].parentNode;
            BlockColumnValues counterValue = this.skipNodes[nowNode.nodeId].getValue();
            int lastRowNum = counterValue.getLastRow();
            System.out.println(nowDir + ":" + lastRowNum);
            int[] counterList = new int[lastRowNum];
            counterValue.create();
            BlockColumnValues colidxValue = this.skipNodes[colidx].getValue();
            colidxValue.create();
            ArrayList<Integer> colidxList = new ArrayList<>();
            int newCounter = 0;
            int nowColidxIndex = 0;
            int nowCounterIndex = 0;
            while(nowCounterIndex < lastRowNum && counterValue.hasNext()){
                int max = counterValue.nextLength();
                HashSet<Integer> removeDoubleSet = new HashSet<>();
                while(colidxValue.hasNext() && nowColidxIndex < max){
                    nowColidxIndex++;
                    Number nowColidx = (Number) colidxValue.next();
                    if(!removeDoubleSet.contains(nowColidx)){
                        removeDoubleSet.add(nowColidx.intValue());
                        colidxList.add(nowColidx.intValue());
                        newCounter++;
                    }

                }
                counterList[nowCounterIndex++] = newCounter;
                removeDoubleSet.clear();
            }

            //2. 多节点跳跃合成counter 和 column index
            while(nowNode.parentNode.nodeId != anceNodeId){
                nowNode = nowNode.parentNode; // 下一个顶点位置
                colidx = this.vertexToColumnIndex.get(nowNode.nodeId);
                nowNode = nowNode.parentNode;
                BlockColumnValues nextCounterValue = this.skipNodes[nowNode.nodeId].getValue();
                BlockColumnValues nextColidxValue = this.skipNodes[colidx].getValue();
                newCounter = 0;
                nowColidxIndex = 0;
                nowCounterIndex = 0;
                ArrayList<Integer> newColidxList = new ArrayList<>();
                int nextLastRow = nextCounterValue.getLastRow();
                int[] newCounterList = new int[nextLastRow];
                nextCounterValue.create();
                nextColidxValue.create();
                while(nowCounterIndex < nextLastRow && nextCounterValue.hasNext()){

                    int max = nextCounterValue.nextLength();
//                    System.out.println(nowCounterIndex + "::" + nextLastRow + " max:" + max);
                    HashSet<Integer> removeDoubleSet = new HashSet<>();
                    while(nextColidxValue.hasNext() && nowColidxIndex < max){
                        nowColidxIndex++;
                        Number nowColidx = (Number) nextColidxValue.next();
                        int lastColidxIndex = counterList[nowColidx.intValue()];
                        int stopIndex = nowColidx.intValue() == counterList.length - 1 ? colidxList.size() : counterList[nowColidx.intValue() + 1];
                        while(lastColidxIndex < stopIndex){
                            if(!removeDoubleSet.contains(colidxList.get(lastColidxIndex))){
                                removeDoubleSet.add(colidxList.get(lastColidxIndex));
                                newCounter++;
                                newColidxList.add(colidxList.get(lastColidxIndex));
                            }
                            lastColidxIndex++;
                        }
                    }
                    removeDoubleSet.clear();
                    newCounterList[nowCounterIndex++] = newCounter;
                }
                colidxList.clear();
                colidxList = newColidxList;
                counterList = newCounterList;
            }
//            System.out.println(dir);

            // 3. 写入文件 写入colidxList和counterList
            FileWriter counterWriter = new FileWriter(nowDir);
            int colidxIndex = 0;
            //第一行为父节点的总个数，用于rollup传递时初始化bitset长度
            counterWriter.write(String.valueOf(counterList.length));
            counterWriter.write("\n");
            //第二行为子节点的总个数，用于drilldown传递时初始化bitset长度
            int max = 0;
            for (int i : colidxList){
                max = Math.max(max, i);
            }
            counterWriter.write(String.valueOf(max));
            counterWriter.write("\n");
            for (int counterBit : counterList) {
                counterWriter.write(String.valueOf(counterBit));
                counterWriter.write("\n");
                while (colidxIndex < counterBit) {
                    counterWriter.write(String.valueOf(colidxList.get(colidxIndex++)));
                    counterWriter.write("\n");
                }
            }
//            counterWriter.write("\n");
//            for (int i = 0; i < colidxList.size(); i++){
//                counterWriter.write(String.valueOf(colidxList.get(i)));
//                counterWriter.write("\n");
//            }
            counterWriter.flush();
            counterWriter.close();
//            bbf.flip();
//            nowFileChannel.write(ByteBuffer.wrap(bbf.array()));
//            nowFileChannel.close();
//            FileInputStream fis = new FileInputStream(dir);
//            FileChannel channel = fis.getChannel();
//            this.indexToReader.put(n1Index + "_" + anceNodeId, channel);
        }else{
            //说明是边的属性，根据边的counter上升到顶点，再进行跳树传递，故不需要做预处理
            return;
        }
    }

    public BitSet bitSetTranShallowFirst(BitSet oldSet, int nowNodeId, int nextNodeId) throws IOException {
        SkipNode nowNode = this.skipNodes[nowNodeId];
        SkipNode nextNode = this.skipNodes[nextNodeId];
        SkipNode deep = nowNode.getNodeLayer() > nextNode.getNodeLayer() ? nowNode : nextNode;
        int deepId = deep.nodeId;
        SkipNode shallow = nowNode.getNodeLayer() > nextNode.getNodeLayer() ? nextNode : nowNode;
        if(shallow.isAncestor(deep)){
            ArrayList<Integer> deepPaths = tranDeepToShallow(deep.nodeId, shallow.getNodeLayer());
            if (deep.nodeId == nowNodeId) {
                return rollup(oldSet, deepPaths);
            } else {
                return drillDown(oldSet,deepPaths);
            }
        }
        else{
            ArrayList<Integer> shallowPath = new ArrayList<>();
            shallowPath.add(shallow.getNodeId());
            int nowSkipHeight = shallow.getSkipHeight();
            while (nowSkipHeight >= 0){
                if(!shallow.getSkipAncestor(nowSkipHeight).isAncestor(deep)){
                    shallow = shallow.getSkipAncestor(nowSkipHeight);
                    nowSkipHeight = shallow.getSkipHeight();
                    shallowPath.add(shallow.getNodeId());
                }
                while(nowSkipHeight != -1 && shallow.getSkipAncestor(nowSkipHeight).isAncestor(deep)){
                    nowSkipHeight--;
                }
                if (nowSkipHeight == -1){
                    shallow = shallow.getSkipAncestor(0);
                    shallowPath.add(shallow.getNodeId());
                }
            }
            ArrayList<Integer> deepPath = tranDeepToShallow(deep.nodeId, shallow.getNodeLayer());

            if (deepId == nowNodeId) {
                long t1 = System.currentTimeMillis();
                BitSet b1 = rollupIncrease(oldSet, deepPath);
                BitSet now =  drillDownIncrease(b1, shallowPath);
                long t2 = System.currentTimeMillis();
                System.out.println("tran time: " + (t2 - t1));
                return now;
            } else {
                long t1 = System.currentTimeMillis();
                BitSet now =  drillDownIncrease(rollupIncrease(oldSet, shallowPath), deepPath);
                long t2 = System.currentTimeMillis();
                System.out.println("tran time: " + (t2 - t1));
                return now;
//                return drillDown(rollup(oldSet, shallowPath), deepPath);
            }
        }
    }

    /**
     * @description 得到从深层转移到浅层的counter
     * @param deepId 层数较大的，较靠近叶子节点的节点的id
     * @param shallowLayer 浅层的层数
     * @return 返回转化路径的ArrayList
     */
    public ArrayList<Integer> tranDeepToShallow(int deepId, int shallowLayer) {
        ArrayList<Integer> paths = new ArrayList<>();
        paths.add(deepId);
        SkipNode deep = this.skipNodes[deepId];
        int anceHeight = deep.getSkipHeight();//当前节点的高度
        while(anceHeight >=0 && (deep.getSkipAncestor(anceHeight) == null ||
                deep.getSkipAncestor(anceHeight).getNodeLayer() < shallowLayer)){
            anceHeight--;
        }
        //进行第一次skip
        SkipNode nowAncestor = deep.getSkipAncestor(anceHeight);
        paths.add(nowAncestor.getNodeId());
        anceHeight = nowAncestor.getSkipHeight();
        while(nowAncestor.getNodeId() != -1 && nowAncestor.getNodeLayer() >= shallowLayer){
            while(anceHeight >=0 && (nowAncestor.getSkipAncestor(anceHeight) == null ||
                    nowAncestor.getSkipAncestor(anceHeight).getNodeLayer() < shallowLayer)){
                anceHeight--;
            }
            if (anceHeight == -1){
                //等于-1说明当前节点层数和shallow节点层数相等
                break;
            }
            nowAncestor = nowAncestor.getSkipAncestor(anceHeight);
            paths.add(nowAncestor.getNodeId());
            anceHeight = nowAncestor.getSkipHeight();
        }
        return paths;
    }

    /**
     * 将当前bitset按照counter上升到目标节点
     * @param nowBitSet 当前节点的bitset
     * @param paths 当前节点到目标节点移动的路径
     * @return 上升后的bitset
     */
    public BitSet rollup(BitSet nowBitSet, ArrayList<Integer> paths) throws IOException {
        int len = paths.size() - 1;
        long t1, t2;
        BitSet newBitSet;
        for (int i = 0; i < len; i++){
            t1 = System.currentTimeMillis();
            if(!this.skipNodes[paths.get(i)].isArray() && this.skipNodes[paths.get(i)].getNodeLayer() == 1){
                continue;
            }
            else if(paths.get(i + 1) != -1 && !this.skipNodes[paths.get(i)].isArray()
                    && this.skipNodes[paths.get(i)].getNodeLayer() - 1 == this.skipNodes[paths.get(i + 1)].getNodeLayer()){
                continue;
            }
            FileChannel fc = this.indexToReader.get(paths.get(i) + "_" + paths.get(i + 1));
            ByteBuffer byteBuffer = ByteBuffer.allocate(this.freeMemory * 1024);
            newBitSet = new BitSet(this.skipNodes[paths.get(i)].getValue().getLastRow());
            int counterIndex = 0;
            int m = nowBitSet.nextSetBit(0);
            int size;
            while (m != -1 && (size = fc.read(byteBuffer)) != -1) {
                byteBuffer.flip();//翻转byteBuffer，改为读模式
                int nowIndex = 0;
                while(m != -1 && nowIndex < size){
                    byte[] bytes = new byte[4];
                    byteBuffer.get(bytes, 0, 4);
                    nowIndex += 4;
                    int nowMax = 0;
                    for(int j = 0; j < 4; j++){
                        nowMax += (bytes[j] & 0xff) << (j * 8);
                    }
                    if(m < nowMax){
                        newBitSet.set(counterIndex);
                    }
                    counterIndex++;
                    if(m + 1 > nowBitSet.length()){
                        m = -1;
                    }
                    while(m != -1 && m < nowMax){
                        m = nowBitSet.nextSetBit(m + 1);
                    }
                }

                byteBuffer.clear();
            }
            nowBitSet = newBitSet;

            t2 = System.currentTimeMillis();
            System.out.println(paths.get(i) + "_" + paths.get(i + 1) + " time: " + (t2 - t1));
        }
        return nowBitSet;
    }

    public BitSet rollupIncrease(BitSet nowBitSet, ArrayList<Integer> paths) throws IOException {
        int len = paths.size() - 1;
        long t1, t2;
        BitSet newBitSet;
        for (int i = 0; i < len; i++){
            t1 = System.currentTimeMillis();
            if(!this.skipNodes[paths.get(i)].isArray() && this.skipNodes[paths.get(i)].getNodeLayer() == 1){
                continue;
            }
            else if(paths.get(i + 1) != -1 && !this.skipNodes[paths.get(i)].isArray()
                    && this.skipNodes[paths.get(i)].getNodeLayer() - 1 == this.skipNodes[paths.get(i + 1)].getNodeLayer()){
                continue;
            }
            FileChannel fc = this.indexToReader.get(paths.get(i) + "_" + paths.get(i + 1));
            ByteBuffer byteBuffer = ByteBuffer.allocate(this.freeMemory * 1024);
            newBitSet = new BitSet(this.skipNodes[paths.get(i)].getValue().getLastRow());
            int counterIndex = 0;
            int m = nowBitSet.nextSetBit(0);
            int size;
            int nowMax = 0;
            while (m != -1 && (size = fc.read(byteBuffer)) != -1) {
                byteBuffer.flip();//翻转byteBuffer，改为读模式
                int nowIndex = 0;
                while(m != -1 && nowIndex < size){
                    byte[] bytes = new byte[2];
                    byteBuffer.get(bytes, 0, 2);
                    nowIndex += 2;
                    int bitNum = 0;
                    for(int j = 0; j < 2; j++){
                        bitNum += (bytes[j] & 0xff) << (j * 8);
                    }
                    nowMax += bitNum;
                    if(m < nowMax){
                        newBitSet.set(counterIndex);
                    }
                    counterIndex++;
                    if(m + 1 > nowBitSet.length()){
                        m = -1;
                    }
                    while(m != -1 && m < nowMax){
                        m = nowBitSet.nextSetBit(m + 1);
                    }
                }

                byteBuffer.clear();
            }
            nowBitSet = newBitSet;

            t2 = System.currentTimeMillis();
            System.out.println(paths.get(i) + "_" + paths.get(i + 1) + " time: " + (t2 - t1));
        }
        return nowBitSet;
    }

    /**
     * 按照counter将bitset下推
     * @param nowBitSet 当前的bitset
     * @param paths 从当前节点到目标节点的counter
     * @return 下推后的bitset
     */
    public BitSet drillDown(BitSet nowBitSet, ArrayList<Integer> paths) throws IOException {
        int len = paths.size() - 1;
        long t1, t2;
        BitSet newBitSet;
        for (int i = len; i > 0; i--){
            t1 = System.currentTimeMillis();
            if(!this.skipNodes[paths.get(i - 1)].isArray() && this.skipNodes[paths.get(i - 1)].getNodeLayer() == 1){
                continue;
            }
            else if(paths.get(i) != -1 && !this.skipNodes[paths.get(i - 1)].isArray()
                    && this.skipNodes[paths.get(i - 1)].getNodeLayer() - 1 == this.skipNodes[paths.get(i)].getNodeLayer()){
                continue;
            }
            newBitSet = new BitSet(this.skipNodes[paths.get(i - 1)].getValue().getLastRow());
            FileChannel fc = this.indexToReader.get(paths.get(i - 1) + "_" + paths.get(i));
            ByteBuffer byteBuffer = ByteBuffer.allocate(this.freeMemory * 1024);
            int lastIndex = 0;
            int readIndex = 0;
            int size;
            while((size = fc.read(byteBuffer)) != -1){
                byteBuffer.flip();
                int nowPosition = 0;
                while(nowPosition < size){
                    byte[] bytes = new byte[4];
                    byteBuffer.get(bytes, 0, 4);
                    nowPosition += 4;
                    int nowIndex = 0;
                    for(int j = 0; j < 4; j++){
                        nowIndex += (bytes[j] & 0xff) << (j*8);
                    }
                    if(nowBitSet.get(readIndex++)){
                        while(lastIndex < nowIndex){
                            newBitSet.set(lastIndex++);
                        }
                    }
                    lastIndex = nowIndex;
                }
                byteBuffer.clear();
            }
            nowBitSet = newBitSet;
            t2 = System.currentTimeMillis();
            System.out.println(paths.get(i - 1) + "_" + paths.get(i) + " time: " + (t2 - t1));
        }
        return nowBitSet;
    }

    public BitSet drillDownIncrease(BitSet nowBitSet, ArrayList<Integer> paths) throws IOException {
        int len = paths.size() - 1;
        long t1, t2;
        BitSet newBitSet;
        for (int i = len; i > 0; i--){
            t1 = System.currentTimeMillis();
            if(!this.skipNodes[paths.get(i - 1)].isArray() && this.skipNodes[paths.get(i - 1)].getNodeLayer() == 1){
                continue;
            }
            else if(paths.get(i) != -1 && !this.skipNodes[paths.get(i - 1)].isArray()
                    && this.skipNodes[paths.get(i - 1)].getNodeLayer() - 1 == this.skipNodes[paths.get(i)].getNodeLayer()){
                continue;
            }
            newBitSet = new BitSet(this.skipNodes[paths.get(i - 1)].getValue().getLastRow());
            FileChannel fc = this.indexToReader.get(paths.get(i - 1) + "_" + paths.get(i));
            ByteBuffer byteBuffer = ByteBuffer.allocate(this.freeMemory * 1024);
            int lastIndex = 0;
            int readIndex = 0;
            int size;
            int nowIndex = 0;
            while((size = fc.read(byteBuffer)) != -1){
                byteBuffer.flip();
                int nowPosition = 0;
                while(nowPosition < size){
                    byte[] bytes = new byte[2];
                    byteBuffer.get(bytes, 0, 2);
                    nowPosition += 2;
                    int nowIndexOffset = 0;
                    for(int j = 0; j < 2; j++){
                        nowIndexOffset += (bytes[j] & 0xff) << (j*8);
                    }
                    nowIndex += nowIndexOffset;
                    if(nowBitSet.get(readIndex++)){
                        while(lastIndex < nowIndex){
                            newBitSet.set(lastIndex++);
                        }
                    }
                    lastIndex = nowIndex;
                }
                byteBuffer.clear();
            }
            nowBitSet = newBitSet;
            t2 = System.currentTimeMillis();
            System.out.println(paths.get(i - 1) + "_" + paths.get(i) + " time: " + (t2 - t1));
        }
        return nowBitSet;
    }

    public ArrayList<Integer> getTranPath(int nowId, int nextId){
        ArrayList<Integer> path = new ArrayList<>();
        int shallowId = -1;
        int deepId = 0;
        if(nowId == -1 || nextId == -1){
            deepId = nowId == -1 ? nextId : nowId;
        }else{
            shallowId = this.skipNodes[nowId].getNodeLayer() > this.skipNodes[nextId].getNodeLayer() ? nextId : nowId;
            deepId = shallowId == nowId ? nextId : nowId;
        }
        while(deepId != shallowId){
            path.add(deepId);
            int height = deepId == -1 ? this.maxTableHeight : this.skipNodes[deepId].getSkipHeight();
            int shallowLayer = shallowId == -1 ? 0 : this.skipNodes[shallowId].getNodeLayer();
            while(height > 0 && this.skipNodes[deepId].getSkipAncestor(height).getNodeLayer() < shallowLayer){
                --height;
            }
            deepId = this.skipNodes[deepId].getSkipAncestor(height).getNodeId();


        }
        path.add(shallowId);
        int nowLayer = nowId == -1 ? 0 : this.skipNodes[nowId].getNodeLayer();
        int nextLayer = nextId == -1 ? 0 : this.skipNodes[nextId].getNodeLayer();
        if(nowLayer < nextLayer){
            //路径由上向下走,需要将path倒转
            ArrayList<Integer> transPath = new ArrayList<>();
            for(int i = path.size() - 1; i >= 0; --i){
                transPath.add(path.get(i));
            }
            return transPath;
        }
        return path;
    }

    public BitSet tranBitset(int nowId, int nextId, BitSet nowBitset) throws IOException {
        System.out.println(nowId + " " + nextId + (columnIndexToVertex.containsKey(nextId) && columnIndexToVertex.get(nextId) == nowId));
        if (columnIndexToVertex.containsKey(nextId) && columnIndexToVertex.get(nextId) == nowId){
            //bitset from vertex trans to column index
            BlockColumnValues colValue = this.skipNodes[nextId].value;
            colValue.create();
            BitSet newBitset = new BitSet(colValue.getLastRow());
            int nowPosInNewBitset = 0;
            while(colValue.hasNext()){
                int nowIndex = ((Number)colValue.next()).intValue();
                if(nowBitset.get(nowIndex)){
                    newBitset.set(nowPosInNewBitset);
                }
                nowPosInNewBitset++;
            }
            return newBitset;
        }else if (columnIndexToVertex.containsKey(nowId) && columnIndexToVertex.get(nowId)==nextId){
            //bitset from column index trans to vertex

            if (nextId == -1){
                nextId = 0;
            }
            BlockColumnValues vertexValue = this.skipNodes[nextId].value;
            BitSet newBitset = new BitSet(vertexValue.getLastRow());
            BlockColumnValues colValue = this.skipNodes[nowId].value;
            colValue.create();
            int p = -1;
            while((p = nowBitset.nextSetBit(p + 1)) != -1){
                colValue.seek(p);
                newBitset.set(((Number)colValue.next()).intValue());
            }
            return newBitset;
        }
        else {
            ArrayList<Integer> tranPath = getTranPath(nowId, nextId);
            BitSet newBitset;
            int nowLayer = nowId == -1 ? 0 : this.skipNodes[nowId].getNodeLayer();
            int nextLayer = nextId == -1 ? 0 : this.skipNodes[nextId].getNodeLayer();
            if(nowLayer < nextLayer){
                //从上向下传，调用drilldown方法
                newBitset = drilldownInGraph(nowBitset, tranPath);
            }else{
                //从下向上传，调用rollup方法
                newBitset = rollupInGraph(nowBitset, tranPath);
            }
            return newBitset;
        }
    }


    /**
     * @description 图数据利用跳树索引进行图中顶点到顶点的rollup跳跃
     * @param nowBitSet 当前节点的bitset
     * @param paths 当前节点到目标节点移动的路径
     * @return 上升后的bitset
     */
    public BitSet rollupInGraph(BitSet nowBitSet, ArrayList<Integer> paths) throws IOException {
        for(int i = 1, len = paths.size(); i < len; i++){
            if(vertex.contains(paths.get(i - 1)) && vertex.contains(paths.get(i))){
                //如果是从顶点到顶点，利用跳树结构进行bitset传递
                String nowDir = dir + paths.get(i - 1) + "_" + paths.get(i) + ".txt";
                BufferedReader br = new BufferedReader(new FileReader(nowDir));
                String line = br.readLine();
                int counterSize = Integer.parseInt(line);
                BitSet newBitset = new BitSet(counterSize);
                //跳过一行，第二行存储的是底层顶点的个数
                br.readLine();

                int counterIndex = 0;
                int last = 0;
                int now;
                while(counterIndex < counterSize){
                    line = br.readLine();
                    now = Integer.parseInt(line);
                    while(last < now){
                        line = br.readLine();
                        ++last;
                        if(nowBitSet.get(Integer.parseInt(line))){
                            newBitset.set(counterIndex);
                            while (last < now){
                                last++;
                                br.readLine();
                            }
                            break;
                        }
                    }
                    ++counterIndex;
                }
                nowBitSet = newBitset;

            }else if(vertex.contains(paths.get(i - 1)) && !vertex.contains(paths.get(i))){
                //如果是从顶点到边，说明是路径的结束，并且该顶点一定是边的直属连接顶点
                // 此时利用column index将信息从顶点中传递出来
                //1.先获得column index对应的列
                int colidx = this.vertexToColumnIndex.get(paths.get(i - 1));
                //2.新建一个bitset，长度为column index字段的长度
                SkipNode colNode = this.skipNodes[colidx];
                BlockColumnValues colidxValue = colNode.getValue();
                int colidxSize = colidxValue.getLastRow();
                BitSet newBitset = new BitSet(colidxSize);
                //3. 读取column index的索引，根据索引判断该位为1还是0
                colidxValue.create();
                int nowPosition = 0;
                while(nowPosition < colidxSize && colidxValue.hasNext()){
                    Number nowIdx = (Number) colidxValue.next();
                    if(nowBitSet.get(nowIdx.intValue())){
                        newBitset.set(nowPosition);
                    }
                    ++nowPosition;
                }
                nowBitSet = newBitset;

            } else if (!this.skipNodes[paths.get(i - 1)].isArray() && !vertex.contains(paths.get(i))) {
                //如果从叶节点到边数组，直接利用一维Counter进行一步传递
                SkipNode edgeNode = this.skipNodes[paths.get(i)];
                BlockColumnValues counter = edgeNode.getValue();
                int counterSize = counter.getLastRow();
                BitSet newBitset = new BitSet(counterSize);

                int m = nowBitSet.nextSetBit(0);
                int n = 0;
                counter.create();
                while (m != -1 && counter.hasNext()) {
                    counter.startRow();
                    int max = counter.nextLength();
                    if (max > m) {
                        newBitset.set(n);
                        if (++m > nowBitSet.length())
                            m = -1;
                        else {
                            m = nowBitSet.nextSetBit(m);
                            while (m != -1 && m < max)
                                m = nowBitSet.nextSetBit(++m);
                        }
                    }
                    n++;
                }
                nowBitSet = newBitset;

            }else {
                //叶节点到顶点的情况，不需要做任何处理，直接返回现在的bitset
                continue;
            }

        }

        return nowBitSet;
    }

    /**
     * @description 图数据利用跳树索引进行图中顶点到顶点的drilldown跳跃
     * @param nowBitSet 当前节点的bitset
     * @param paths 当前节点到目标节点移动的路径
     * @return 上升后的bitset
     */
    public BitSet drilldownInGraph(BitSet nowBitSet, ArrayList<Integer> paths) throws IOException {
        for(int i = 1; i < paths.size(); i++){
            if (vertex.contains(paths.get(i - 1)) && vertex.contains(paths.get(i))){
                //从顶点节点向顶点节点drilldown,使用与计算的counter和column index进行传递
                String nowDir = dir + paths.get(i) + "_" + paths.get(i - 1) + ".txt";
                BufferedReader br = new BufferedReader(new FileReader(nowDir));
                //文件第一行存储的是上层节点的个数，因此首先跳过一行
                br.readLine();
                //文件第二行存储的是下层节点的数据个数，用来初始化目标bitset的长度
                String line = br.readLine();
                BitSet newBitset = new BitSet(Integer.parseInt(line));
                int m = -1;
                int now = 0;
                int nowIndex = 0;
                int lastIndex = 0;
                while((m = nowBitSet.nextSetBit(m + 1)) != -1){
                    while(now < m){
                        line = br.readLine();
                        nowIndex = Integer.parseInt(line);

                        ++now;
                        while(lastIndex < nowIndex){
                            br.readLine();
                            ++lastIndex;
                        }
                    }
                    nowIndex = Integer.parseInt(br.readLine());
                    while(lastIndex < nowIndex){
                        newBitset.set(Integer.parseInt(br.readLine()));
                        ++lastIndex;
                    }
                    ++now;
                }
                nowBitSet = newBitset;
//                int setNum = -1;
//                int count = 0;
//                while((setNum = nowBitSet.nextSetBit(setNum + 1))!=-1){
//                    ++count;
//                }
//                System.out.println(count);

            }else if(this.vertex.contains(paths.get(i - 1))){
                //从顶点向非顶点传递,不做任何处理
                continue;

            } else if (this.skipNodes[paths.get(i - 1)].isArray() && !this.skipNodes[paths.get(i)].isArray()){
//                System.out.println("i - 1:" + paths.get(i - 1) + " i:" + paths.get(i));
                //从边传递到叶节点，按照传统功夫的drilldown传递
                BlockColumnValues counterValue = this.skipNodes[paths.get(i - 1)].value;
                BitSet newBitset = new BitSet(counterValue.getLastRow());
                int p = nowBitSet.nextSetBit(0);
                int nowCounterIndex = 0;
                int lastValue = 0;
                int nowValue = 0;
                counterValue.create();

                while(p != -1){
                    while(nowCounterIndex < p){
                        nowValue = counterValue.nextLength();
                        ++nowCounterIndex;
                    }
                    lastValue = nowValue;
                    nowValue = counterValue.nextLength();
                    while(lastValue < nowValue){
                        newBitset.set(++lastValue);
                    }
                    p = nowBitSet.nextSetBit(p + 1);
                }
                nowBitSet = newBitset;

            }else if(!this.vertex.contains(paths.get(i - 1)) && this.skipNodes[paths.get(i - 1)].isArray() && vertex.contains(paths.get(i))){
                //从边传递到顶点，那么该顶点是边的目标顶点
                //此时通过column index向下传递
                int colidx = vertexToColumnIndex.get(paths.get(i)); // 目标顶点对应的column index

                //初始化newBitset，大小为下一个顶点的长度
                BitSet newBitset = new BitSet(this.skipNodes[paths.get(i)].value.getLastRow());
                BlockColumnValues colValue = this.skipNodes[colidx].value;
                BlockColumnValues counterValue = this.skipNodes[paths.get(i - 1)].value;
                counterValue.create();
                colValue.create();
                int p = nowBitSet.nextSetBit(0);
                int nowPositionInCounter = 0;
                int lastPositionInColIdx = 0;
                int beginPositionInColIdx = 0;
                int nowPositionInColIdx = 0;
                while(p != -1){
                    while(nowPositionInCounter < p){
                        beginPositionInColIdx = nowPositionInColIdx;
                        nowPositionInColIdx = ((Number)counterValue.nextLength()).intValue();
                        ++nowPositionInCounter;
                    }
                    while(lastPositionInColIdx < beginPositionInColIdx){
                        ++lastPositionInColIdx;
                        colValue.next();
                    }
                    while(lastPositionInColIdx < nowPositionInColIdx){
                        ++lastPositionInColIdx;
                        newBitset.set(((Number)colValue.next()).intValue());
                    }
                    lastPositionInColIdx = nowPositionInColIdx;
                    p = nowBitSet.nextSetBit(p + 1);
                }
                nowBitSet = newBitset;

            }else{
                //从顶点直接向属性传递，无需进行任何操作
                continue;
            }
        }

        return nowBitSet;
    }

    @Override
    public String toString() {
        StringBuilder tableString = new StringBuilder();
        tableString.append("node number: ").append(this.nodeNumber).append("\n");
        tableString.append("max layer: ").append(this.maxLayer).append("\n");
        tableString.append("skip table max height: ").append(this.maxTableHeight).append("\n");
        tableString.append("root:");
        tableString.append("root id:").append(root.getNodeId()).append(" root layer:").append(root.getNodeLayer()).append(" root skip height:").append(root.skipHeight).append("\n");

        for (SkipNode node : this.skipNodes){
            tableString.append(node.toString()).append("\n");
        }
        return tableString.toString();
    }

}
