package quest;

import columnar.BatchColumnFileReader;
import columnar.BlockColumnValues;
import cores.avro.FilterOperator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;

public abstract class DataFileReader {

    BatchColumnFileReader reader;
    GenericData model;
    BlockColumnValues[] values;
    int[] readNO; // column number
    int[] arrayWidths; //每一列对应一个width，表明这一列的宽度，如果节点为数组，宽度为孩子宽度的和，默认为1
    int column;
    int[] arrayValues;
    HashMap<String, Integer> columnsByName;
    Schema readSchema;
    FilterOperator[] filters; //ensure that the filters is sorted from small to big layer

    String readParent;
    HashMap<String, Integer> readLength;
    Object[][] readValue;
    String currentParent;
    int currentColumnIndex;
    int currentLayer;
    BitSet filterSet;
    ArrayList<BitSet> chooseSet;
    HashMap<String, Integer> bitSetMap;
    HashMap<Integer, BitSet> filterSetMap = new HashMap<>();
    int[] readIndex; //the index of the next Record in every column;
    int all; //the number of the remain values in the disk;
    int[] setStart; //the start of the bitset of every layer
    int[] readSet; //the index of chooseSet of every read column
    boolean noFilters; // 是否有filter
    int currentMax;
    static int max = 100000;

    SkipTree skipTable;

    String dir;

    public void createSchema(Schema s) throws IOException {

    }

    public void setMetaDir(String dir){
        this.dir = dir;
    }

    public abstract void filter() throws IOException;

    /**
     * @description bitset trans cross multiple model
     */
    public void crossModel(DataFileReader anoReader, String anoField, String myField) throws IOException {
        int anoId = anoReader.columnsByName.get(anoField);
        int myId = this.columnsByName.get(myField);
        currentParent = values[myId].getParentName();
        BlockColumnValues anoValue = anoReader.skipTable.skipNodes[anoId].value;
        BlockColumnValues myValue = this.skipTable.skipNodes[myId].value;
        BitSet nowBitset = anoReader.filterSet;
        HashSet<Long> crossSet = new HashSet<>();
        anoValue.create();
        BitSet newBitset = new BitSet(myValue.getLastRow());
        int index = 0;
        int p = -1;
        while ((p = nowBitset.nextSetBit(p + 1))!=-1){
            while (index < p){
                index++;
                anoValue.next();
            }
            crossSet.add(((Number)anoValue.next()).longValue());
            index++;
        }

        myValue.create();
        index = 0;
        while(myValue.hasNext()){
            long nowValue = ((Number)myValue.next()).longValue();
            if(crossSet.contains(nowValue)){
                newBitset.set(index);
            }
            ++index;

        }
        this.filterSet = newBitset;
        if(filterSetMap.containsKey(myId)){
            this.filterSet.and(filterSetMap.get(myId));
        }
        filterSetMap.put(myId, (BitSet)this.filterSet.clone());
        index=0;
        while ((p = filterSet.nextSetBit(p + 1)) != -1){
            index++;
        }
        System.out.println("cross bitset true num :" + index);
        System.out.println("before cross: " + crossSet.size());
    }


    public void crossModel(DataFileReader anoReader, String anoField, String myField, boolean flag) throws IOException {
        int anoId = anoReader.columnsByName.get(anoField);
        int myId = this.columnsByName.get(myField);
        currentParent = values[myId].getParentName();
//        int col = columnsByName.get(currentParent);
//        currentParent = values[col].getParentName();
        BlockColumnValues anoValue = anoReader.values[anoId];
        BlockColumnValues myValue = values[myId];
        BitSet nowBitset = anoReader.filterSet;
        HashSet<Long> crossSet = new HashSet<>();
        anoValue.create();
        BitSet newBitset = new BitSet(myValue.getLastRow());
        int index = 0;
        int p = -1;
        while ((p = nowBitset.nextSetBit(p + 1))!=-1){
            while (index < p){
                index++;
                anoValue.next();
            }
            crossSet.add(((Number)anoValue.next()).longValue());
            index++;
        }

        myValue.create();
        index = 0;
        while(myValue.hasNext()){
            long nowValue = ((Number)myValue.next()).longValue();
            if(crossSet.contains(nowValue)){
                newBitset.set(index);
            }
            ++index;

        }
        this.filterSet = newBitset;
        if(filterSetMap.containsKey(myId)){
            this.filterSet.and(filterSetMap.get(myId));
        }
        filterSetMap.put(myId, (BitSet)this.filterSet.clone());
        index=0;
        while ((p = filterSet.nextSetBit(p + 1)) != -1){
            index++;
        }
        System.out.println("cross bitset true num :" + index);
        System.out.println("before cross: " + crossSet.size());
    }

}
