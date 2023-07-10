package quest;

import codec.metadata.FileColumnMetaData;
import columnar.BatchColumnFileReader;
import columnar.BlockColumnValues;
import cores.avro.AvroColumnator;
import cores.avro.FilterOperator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.trevni.TrevniRuntimeException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static cores.avro.AvroColumnator.isSimple;

public class GraphDataFileReader<D> extends DataFileReader implements Closeable {

    ArrayList<Integer> path = new ArrayList<>(); //bitset 传递路径
    int nowPathIndex; //当前bitset所在路径中的第i个位置

    HashSet<Integer> vertex;    // 记录schema中所有的顶点，不包含根节点
    HashSet<Integer> columnIndex; // 记录schema中所有column index列
    HashMap<Integer, Integer> vertexToColumnIndex; // 实体顶点节点对应的同一层中的column index
    HashMap<Integer, Integer> columnIndexToVertex; // 每个column index节点对应的实体节点


    public GraphDataFileReader(File file, FilterOperator[] filters,ArrayList<String> pathWithName, ArrayList<String> vertex, ArrayList<String> columnIndex,
                               HashMap<String, String> vertexToColumnIndex, HashMap<String, String> columnIndexToVertex) throws IOException {
        this.model = GenericData.get();
        this.reader = new BatchColumnFileReader(file);
        this.filters = filters;
        columnsByName = reader.getColumnsByName(); //new HashMap<String, Integer>
        this.values = new BlockColumnValues[reader.getColumnCount()]; //开辟空间
        for (int i = 0; i < values.length; i++) {
            values[i] = reader.getValues(i);
        }
        noFilters = (filters == null);

        HashSet<Integer> vertexSet = new HashSet<>();
        HashSet<Integer> columnIndexSet = new HashSet<>();
        HashMap<Integer, Integer> columnIndexToVertexMap = new HashMap<>();
        HashMap<Integer, Integer> vertexToColumnIndexMap = new HashMap<>();

        for (String vertexName : vertex){
            int vertexId = this.columnsByName.get(vertexName);
            int columnIndexId = this.columnsByName.get(vertexToColumnIndex.get(vertexName));
            vertexSet.add(vertexId);
            vertexToColumnIndexMap.put(vertexId, columnIndexId);
        }
        vertexSet.add(-1);

        for(String columnIndexName : columnIndex){
            int columnIndexId = this.columnsByName.get(columnIndexName);
            int vertexId = Objects.equals(columnIndexToVertex.get(columnIndexName), "root") ? -1 : this.columnsByName.get(columnIndexToVertex.get(columnIndexName));
            columnIndexSet.add(columnIndexId);
            columnIndexToVertexMap.put(columnIndexId, vertexId);
        }

        //将字符串路径初始化为id路径
        for(String columnName : pathWithName){
            int columnIndexId = Objects.equals(columnName, "root") ? -1 : this.columnsByName.get(columnName);
            this.path.add(columnIndexId);
        }

        //
        this.vertex = vertexSet;

        this.columnIndex = columnIndexSet;

        this.vertexToColumnIndex = vertexToColumnIndexMap;

        this.columnIndexToVertex = columnIndexToVertexMap;
    }

    public void createSchema(Schema s, boolean genMeta) throws IOException {
        readSchema = s;  // schema格式
        AvroColumnator readColumnator = new AvroColumnator(s);
        FileColumnMetaData[] readColumns =  readColumnator.getColumns();
//        for (FileColumnMetaData meta:readColumns){
//            System.out.println(meta);
//        }

        this.skipTable = new SkipTree(readColumns, values, vertex, columnIndex, vertexToColumnIndex, columnIndexToVertex, this.dir, genMeta);

//        System.out.println(this.skipTable);
//        System.out.println("*********************************");
        this.arrayWidths = readColumnator.getArrayWidths();

        this.readNO = new int[readColumns.length];
        for (int i = 0; i < readColumns.length; i++) {
            this.readNO[i] = reader.getColumnNumber(readColumns[i].getName());
        }
        this.readParent = values[readNO[0]].getParentName();
//        System.out.println(Arrays.toString(this.arrayWidths));
//        System.out.println(Arrays.toString(this.readNO));
//        System.out.println(this.readParent);

    }

    @Override
    public void createSchema(Schema s) throws IOException {
        readSchema = s;  // schema格式
        AvroColumnator readColumnator = new AvroColumnator(s);
        FileColumnMetaData[] readColumns =  readColumnator.getColumns();
//        for (FileColumnMetaData meta:readColumns){
//            System.out.println(meta);
//        }

        this.skipTable = new SkipTree(readColumns, values, vertex, columnIndex, vertexToColumnIndex, columnIndexToVertex, this.dir, false);

//        System.out.println(this.skipTable);
//        System.out.println("*********************************");
        this.arrayWidths = readColumnator.getArrayWidths();

        this.readNO = new int[readColumns.length];
        for (int i = 0; i < readColumns.length; i++) {
            this.readNO[i] = reader.getColumnNumber(readColumns[i].getName());
        }
        this.readParent = values[readNO[0]].getParentName();
//        System.out.println(Arrays.toString(this.arrayWidths));
//        System.out.println(Arrays.toString(this.readNO));
//        System.out.println(this.readParent);

    }

    public void tranBitset(String targetField) throws IOException {

        if (targetField.compareTo("root") == 0){
            filterSet = this.skipTable.tranBitset( currentColumnIndex, -1, filterSet);
            currentColumnIndex = -1;
            if(filterSetMap.containsKey(-1)){
                filterSet.and(filterSetMap.get(-1));
            }
            filterSetMap.put(-1, (BitSet) filterSet.clone());
        }else{
            int tm = columnsByName.get(targetField);
            currentParent = values[tm].getParentName();
            currentLayer = values[tm].getLayer();
            filterSet = this.skipTable.tranBitset( currentColumnIndex, tm, filterSet);

            chooseSet = new ArrayList<>();
            chooseSet.add((BitSet) filterSet.clone());
            bitSetMap = new HashMap<>();
            bitSetMap.put(currentParent, 0);
        }


    }

    /**
     * 函数体内主要处理第一个过滤器
     * 进行第一个过滤器时初始化全局filterSet
     * @throws IOException ...
     */
    public void filter() throws IOException {
        if(filters == null || filters.length == 0)return;
        String column = filters[0].getName(); //得到过滤器的相关字段
        Integer tm = columnsByName.get(column);
        if (tm == null)
            throw new TrevniRuntimeException("No filter column named: " + column);
        //values[tm].getLastRow() 返回columnDescriptor 中的行数
        if(filterSet == null){
            filterSet = new BitSet(values[tm].getLastRow());
        }
        currentParent = values[tm].getParentName();
        this.currentColumnIndex = tm;
        currentLayer = values[tm].getLayer();
        int i = 0;
        //        values[tm].createTime();
        //        values[tm].createSeekBlock();
        values[tm].create();
        while (values[tm].hasNext()) {
            if (filters[0].isMatch(values[tm].next())) {
                filterSet.set(i);
            }
            i++;
        }

        for (int c = 1; c < filters.length; c++) {
            filter(c);

        }

        chooseSet = new ArrayList<>();
        chooseSet.add((BitSet) filterSet.clone());
        bitSetMap = new HashMap<>();
        bitSetMap.put(currentParent, 0);
    }

    /**
     * 执行第c个过滤器，其中c>0 && c小于过滤器个数-1 ， 即c指代的过滤器不是最后一个过滤器
     * @param c 执行的过滤器在过滤器列表中的index
     * @throws IOException ...
     */
    private void filter(int c) throws IOException {
        String column = filters[c].getName();
        Integer tm = columnsByName.get(column);
        if (tm == null)
            throw new TrevniRuntimeException("No filter column named: " + column);
//        String parent = values[tm].getParentName();
//        int layer = values[tm].getLayer();
//        if (layer != currentLayer || (parent != null && !currentParent.equals(parent))) {
//            //需要调用rollup和drill down方法
//            filterSetTran(tm);
//        }
        long t1 = System.currentTimeMillis();
        while(!Objects.equals(path.get(nowPathIndex), tm)){
            System.out.println("path:" + path.get(nowPathIndex) + "  " + path.get(nowPathIndex + 1));
            filterSet = this.skipTable.tranBitset(path.get(nowPathIndex), path.get(nowPathIndex + 1), filterSet);

            if(needAndFilter(path.get(nowPathIndex), path.get(nowPathIndex + 1)) && path.get(nowPathIndex) != -1){
                if(path.get(nowPathIndex + 1) == -1 || this.skipTable.skipNodes[path.get(nowPathIndex)].getNodeLayer() > this.skipTable.skipNodes[path.get(nowPathIndex + 1)].getNodeLayer()){
                    //bitset trans by rollup function
                    if (vertex.contains(path.get(nowPathIndex + 1))){
                        //rollup to vertex
                        if (filterSetMap.containsKey(path.get(nowPathIndex + 1))){
                            // rollup from vertex to vertex, need &
                            filterSet.and(filterSetMap.get(path.get(nowPathIndex + 1)));
                        }
                        filterSetMap.put(path.get(nowPathIndex + 1), (BitSet) filterSet.clone());
                    }
                }
            }
            ++nowPathIndex;
        }

        long t2 = System.currentTimeMillis();
        System.out.println(c + "th trans time: " + (t2 - t1));
        this.currentColumnIndex = tm;
        currentLayer = values[tm].getLayer();
        currentParent = values[tm].getParentName();
        int m = filterSet.nextSetBit(0);//从0开始检查下一个真实位的索引
        //        values[tm].createTime();
        //        values[tm].createSeekBlock();
        values[tm].create();
        while (m != -1) {
            values[tm].seek(m);
            if (!filters[c].isMatch(values[tm].next())) {
                filterSet.set(m, false);
            }
            if (++m > filterSet.length())
                break;
            m = filterSet.nextSetBit(m);
        }

        if (nowPathIndex < path.size() - 1){
            filterSet = this.skipTable.tranBitset(path.get(nowPathIndex), path.get(nowPathIndex), filterSet);
            if(needAndFilter(path.get(nowPathIndex), path.get(nowPathIndex + 1)) && path.get(nowPathIndex) != -1){
                if(path.get(nowPathIndex + 1) == -1 || this.skipTable.skipNodes[path.get(nowPathIndex)].getNodeLayer() > this.skipTable.skipNodes[path.get(nowPathIndex + 1)].getNodeLayer()){
                    //bitset trans by rollup function
                    if (vertex.contains(path.get(nowPathIndex + 1))){
                        //rollup to vertex
                        if (filterSetMap.containsKey(path.get(nowPathIndex + 1))){
                            // rollup from vertex to vertex, need &
                            filterSet.and(filterSetMap.get(path.get(nowPathIndex + 1)));
                        }
                        filterSetMap.put(path.get(nowPathIndex + 1), (BitSet) filterSet.clone());
                    }
                }
            }
            nowPathIndex++;
        }
        //        timeIO += values[tm].getTime();
        //        blockTime.addAll(values[tm].getBlockTime());
        //        blockStart.addAll(values[tm].getBlockStart());
        //        blockEnd.addAll(values[tm].getBlockEnd());
        //        blockOffset.addAll(values[tm].getBlockOffset());
        //        readBlockSize += values[tm].getSeekBlock()[0];
        //        seekedBlock += values[tm].getSeekBlock()[1];
        //        blockCount += values[tm].getBlockCount();
    }

    public boolean needAndFilter(int nowId, int nextId){
        if (columnIndexToVertex.containsKey(nextId) && columnIndexToVertex.get(nextId) == nowId){
            return false;
        }
        return !columnIndexToVertex.containsKey(nowId) || columnIndexToVertex.get(nowId) != nextId;

    }

    /**
     * 将当前的filter bitset rollup到上一层
     * @param array 当前要过滤的列的父亲列名称
     * @throws IOException ...
     */
    private void upTran(String array) throws IOException {
        int col = columnsByName.get(array);
        BitSet set = new BitSet(values[col].getLastRow());
        int m = filterSet.nextSetBit(0);
        int n = 0;
        //        values[col].createTime();
        //        values[col].createSeekBlock();
        values[col].create();
        while (m != -1 && values[col].hasNext()) {
            values[col].startRow();
            int max = values[col].nextLength();
            if (max > m) {
                set.set(n);
                if (++m > filterSet.length())
                    m = -1;
                else {
                    m = filterSet.nextSetBit(m);
                    while (m != -1 && m < max)
                        m = filterSet.nextSetBit(++m);
                }
            }
            n++;
        }
        filterSet = set;
    }

    private void downTran(String array) throws IOException {
        int col = columnsByName.get(array);
        BitSet set = new BitSet(values[col + 1].getLastRow());
        int p = filterSet.nextSetBit(0);
        int q = -1;
        values[col].create();
        if (p == 0) {
            values[col].startRow();
            int[] res = values[col].nextLengthAndOffset();
            if (res[0] > 0)
                set.set(res[1], res[0] + res[1]);
            q = p;
            p = filterSet.nextSetBit(1);
        }
        while (p != -1) {
            if (p == q + 1) {
                values[col].startRow();
                int[] res = values[col].nextLengthAndOffset();
                if (res[0] > 0)
                    set.set(res[1], res[0] + res[1]);
                //                for (int j = 0; j < res[0]; j++)
                //                    set.set(j + res[1]);
            } else {
                values[col].seek(p - 1);
                values[col].startRow();
                values[col].nextLengthAndOffset();
                values[col].startRow();
                int[] res = values[col].nextLengthAndOffset();
                if (res[0] > 0)
                    set.set(res[1], res[0] + res[1]);
                //                for (int j = 0; j < res[0]; j++)
                //                    set.set(j + res[1]);
            }
            q = p;
            if (++p > filterSet.length())
                break;
            p = filterSet.nextSetBit(p);
        }
        filterSet = set;
    }

    public void createFilterRead(int max) throws IOException {
        assert (!noFilters);
        this.max = max;
//        System.out.println(readNO.length);
        for (int i = 0; i < readNO.length; i++) {
            //            values[readNO[i]].createTime();
            //            values[readNO[i]].createSeekBlock();
            values[readNO[i]].create();
        }
        readValue = new Object[readNO.length][];
        readLength = new HashMap<String, Integer>();
        readImplPri();
    }

    private void readImplPri() throws IOException {
//        long start = System.currentTimeMillis();
        setStart = new int[readNO.length];
        readSet = new int[readNO.length];
        int layer = values[readNO[0]].getLayer();
        String parent = values[readNO[0]].getParentName();
        if (layer != currentLayer || (parent != null && !currentParent.equals(parent)))
            readSetTran(readNO[0]);
        all = filterSet.cardinality();
        if (all > max) {
            readLength.put(readParent, max);
        } else {
            readLength.put(readParent, all);
        }
        for (int i = 0; i < readNO.length; i++) {
            parent = values[readNO[i]].getParentName();
            Integer set = bitSetMap.get(parent);
            if (set == null) {
                readSetTran(readNO[i]);
                readSet[i] = chooseSet.size() - 1;
            } else {
                readSet[i] = set;
                filterSet = chooseSet.get(set);
                currentParent = parent;
                currentLayer = values[readNO[i]].getLayer();
            }
            setStart[i] = filterSet.nextSetBit(0);
        }
//        filterSetMap.clear();
//        filterSetMap = null;
//        long end = System.currentTimeMillis();
//        System.out.println("read set tran time: " + (end - start));

        for (int i = 0; i < readNO.length; i++) {
            filterSet = chooseSet.get(readSet[i]);
            currentParent = values[readNO[i]].getParentName();
            currentLayer = values[readNO[i]].getLayer();
            readPri(i);
        }
        all -= readLength.get(readParent);
        readIndex = new int[readNO.length];
    }

    private void readSetTran(int c) throws IOException {
        List<String> left = new ArrayList<String>();
        List<String> right = new ArrayList<String>();
        int layer = values[c].getLayer();
        String parent = values[c].getParentName();
        if (currentLayer > layer) {
            for (int i = currentLayer; i > layer; i--) {
                left.add(currentParent);
                int col = columnsByName.get(currentParent);
                currentParent = values[col].getParentName();
            }
            currentLayer = layer;
        }
        if (layer > currentLayer) {
            for (int i = layer; i > currentLayer; i--) {
                right.add(parent);
                int col = columnsByName.get(parent);
                parent = values[col].getParentName();
            }
            layer = currentLayer;
        }
        while (currentParent != null && !currentParent.equals(parent)) {
            left.add(currentParent);
            int l = columnsByName.get(currentParent);
            currentParent = values[l].getParentName();
            right.add(parent);
            int r = columnsByName.get(parent);
            parent = values[r].getParentName();
        }

        for (int i = 0; i < left.size(); i++) {
            String array = left.get(i);
            upTran(array);
            String arr = values[columnsByName.get(array)].getParentName();
            bitSetMap.put(arr, chooseSet.size());
            chooseSet.add(filterSet);
        }

        for (int i = right.size() - 1; i >= 0; i--) {
            String array = right.get(i);
            downTran(array);
            bitSetMap.put(array, chooseSet.size());
//            BitSet f = filterSetMap.get(array);
//            if (f != null) {
//                filterSet.and(f);
//            }
            chooseSet.add(filterSet);
        }
        currentLayer = values[c].getLayer();
        currentParent = values[c].getParentName();
    }

    private void readPri(int c) throws IOException {
        int length = readLength.get(currentParent);
        readValue[c] = new Object[length];
        if (values[readNO[c]].isArray()) {
            BitSet set = chooseSet.get(readSet[c + 1]);
            int changeArr = 0;
            int in = 0;
            int p = setStart[c];
            //            int q = -1;
            int m = setStart[c + 1];
            while (in < length) {
                values[readNO[c]].seek(p);
                values[readNO[c]].startRow();
                int res = values[readNO[c]].nextLength();
                int re = 0;
                while (m != -1 && res > m) {
                    ++re;
                    m = set.nextSetBit(++m);
                }
                changeArr += re;
                readValue[c][in] = re;
                in++;
                p = filterSet.nextSetBit(++p);
            }
            readLength.put(values[readNO[c]].getName(), changeArr);
            setStart[c] = p;
        } else {
            int in = 0;
            int m = setStart[c];
            while (in < length) {
                values[readNO[c]].seek(m);
                readValue[c][in] = values[readNO[c]].next();
                in++;
                m = filterSet.nextSetBit(++m);
            }
            setStart[c] = m;
        }
    }
    public int getRowCount(int columnNo) {
        return values[readNO[columnNo]].getLastRow();
    }

    public boolean hasNext() {
        return readIndex[0] < readValue[0].length || all > 0;
    }

    public D next() {
        try {
            if (readIndex[0] == readValue[0].length) {
                if (noFilters)
                    readImplPriNoFilters();
                else
                    readImpl();
            }
            column = 0;
            return (D) read(readSchema);
        } catch (IOException e) {
            throw new TrevniRuntimeException(e);
        }
    }

    private void readImpl() throws IOException {
        if (all == 0)
            return;
        if (all > max) {
            readLength.put(readParent, max);
        } else {
            readLength.put(readParent, all);
        }
        for (int i = 0; i < readNO.length; i++) {
            filterSet = chooseSet.get(readSet[i]);
            currentParent = values[readNO[i]].getParentName();
            currentLayer = values[readNO[i]].getLayer();
            readPri(i);
        }
        all -= readLength.get(readParent);
        readIndex = new int[readNO.length];
    }

    public Object read(Schema s) throws IOException {
        if (isSimple(s)) {
            return readValue(s, column++);
        }
        final int startColumn = column;

        switch (s.getType()) {
            case RECORD:
                Object record = model.newRecord(null, s);
                for (Schema.Field f : s.getFields()) {
                    Object value = read(f.schema());
                    model.setField(record, f.name(), f.pos(), value);
                }
                return record;
            case ARRAY:
                int length = (int) readValue[column][readIndex[column]++];

                //                values[readNO[column]].startRow();
                //                int[] rr = values[readNO[column]].nextLengthAndOffset();
                //                length = rr[0];
                List elements = (List) new GenericData.Array(length, s);
                for (int i = 0; i < length; i++) {
                    this.column = startColumn;
                    Object value;
                    if (isSimple(s.getElementType()))
                        value = readValue(s, readNO[++column]);
                    else {
                        column++;
                        value = read(s.getElementType());
                    }
                    elements.add(value);
                }
                column = startColumn + arrayWidths[startColumn];
                return elements;
            default:
                throw new TrevniRuntimeException("Unknown schema: " + s);
        }
    }

    public Object readValue(Schema s, int column) throws IOException {
        Object v = readValue[column][readIndex[column]++];

        switch (s.getType()) {
            case ENUM:
                return model.createEnum(s.getEnumSymbols().get((Integer) v), s);
            case FIXED:
                return model.createFixed(null, ((ByteBuffer) v).array(), s);
        }

        return v;
    }

    private void readImplPriNoFilters() throws IOException {
        if (all == 0)
            return;
        if (all > max) {
            currentMax = max;
            readLength.put(readParent, max);
        } else {
            currentMax = all;
            readLength.put(readParent, all);
        }
        readImplNoFilters();
        all -= readLength.get(readParent);
        readIndex = new int[readNO.length];
    }

    private void readImplNoFilters() throws IOException {
        for (int i = 0; i < readNO.length; i++) {
            currentMax = readLength.get(values[readNO[i]].getParentName());
            readValue[i] = new Object[currentMax];
            if (values[readNO[i]].isArray()) {
                int j = 0;
                int[] lenAndOff = new int[2];
                values[readNO[i]].startRow();
                lenAndOff = values[readNO[i]].nextLengthAndOffset();
                readValue[i][j++] = lenAndOff[0];
                int off = lenAndOff[1];
                while (j < currentMax) {
                    values[readNO[i]].startRow();
                    lenAndOff = values[readNO[i]].nextLengthAndOffset();
                    readValue[i][j++] = lenAndOff[0];
                }
                readLength.put(values[readNO[i]].getName(), lenAndOff[0] + lenAndOff[1] - off);
            } else {
                int j = 0;
                while (j < currentMax) {
                    try {
                        readValue[i][j++] = values[readNO[i]].next();
                    } catch (Exception e) {
                        throw new TrevniRuntimeException(
                                "array overleave" + i + ":" + readValue.length + ":" + j + ":"
                                        + readValue[i].length + ":" + readNO[i] + ":" + values.length + ":"
                                        + readNO.length);
                    }
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
