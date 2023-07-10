package experiment.data_gen;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;

import cores.avro.BatchAvroColumnWriter;
import cores.avro.ComparableKey;
import cores.avro.SortedAvroReader;
import cores.avro.SortedAvroWriter;

public class DataTran {
    public static void lSort(String path, String schema, int[] fields, String resultPath, int free, int mul)
            throws IOException {
//        lSort(path + "lineitem.tbl", schema + "1/single.avsc", fields0, result + "1/", free, mul);
        Schema l = new Schema.Parser().parse(new File(schema));
        List<Field> fs = l.getFields();
        SortedAvroWriter<ComparableKey, Record> writer = new SortedAvroWriter<ComparableKey, Record>(resultPath, l,
                free, mul);
        BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
        String line;
        int index = 0;
        while ((line = reader.readLine()) != null) {
            index++;
            String[] tmp = line.split("\\|");
            Record data = new Record(l);
            for (int i = 0; i < fs.size(); i++) {
                switch (fs.get(i).schema().getType()) {
                    case INT:
                        data.put(i, Integer.parseInt(tmp[i]));
                        break;
                    case LONG:
                        data.put(i, Long.parseLong(tmp[i]));
                        break;
                    case FLOAT:
                        data.put(i, Float.parseFloat(tmp[i]));
                        break;
                    case DOUBLE:
                        data.put(i, Double.parseDouble(tmp[i]));
                        break;
                    case BYTES:
                        data.put(i, ByteBuffer.wrap(tmp[i].getBytes()));
                        break;
                    default:
                        data.put(i, tmp[i]);
                }

            }
            writer.append(new ComparableKey(data, fields), data);
        }
        System.out.println(index);
        reader.close();
        writer.flush();
        System.gc();
    }

    public static void doublePri(String path1, String path2, String schema1, String schema2, int[] fIn1, int[] fIn2,
            int[] fOut, String resultPath, int free, int mul) throws IOException {
        Schema s1 = new Schema.Parser().parse(new File(schema1 + "single.avsc"));
        Schema s = new Schema.Parser().parse(new File(schema1 + "nest.avsc"));
        Schema s2 = new Schema.Parser().parse(new File(schema2));
        List<Field> fs1 = s1.getFields();

        SortedAvroReader reader1 = new SortedAvroReader(path1, s1, fIn1);
        SortedAvroReader reader2 = new SortedAvroReader(path2, s2, fIn2);

        SortedAvroWriter<ComparableKey, Record> writer = new SortedAvroWriter<ComparableKey, Record>(resultPath, s,
                free, mul);
        Record r2 = reader2.next();
        ComparableKey k2 = new ComparableKey(r2, fIn2);
        while (reader1.hasNext()) {
            //Partsupp表
            Record r1 = reader1.next();
            Record data = new Record(s);
            for (int i = 0; i < fs1.size(); i++) {
                data.put(i, r1.get(i));
            }
            ComparableKey k1 = new ComparableKey(data, fIn1);
            List<Record> arr = new ArrayList<Record>();
            while (k2 != null && k1.compareTo(k2) == 0) {
                arr.add(r2);
//                System.out.println(r2);
                if (reader2.hasNext()) {
                    r2 = reader2.next();
                    k2 = new ComparableKey(r2, fIn2);
                } else {
                    k2 = null;
                    reader2.close();
                    break;
                }
            }
            data.put(fs1.size(), arr);
            writer.append(new ComparableKey(data, fOut), data);
        }
        reader1.close();
        writer.flush();
        System.gc();
    }

    public static void newDoublePri(String parentPath, String parentSchema, String childPath, String childSchema,
                             String resultPath, String resultSchema, int[] fIn1, int[] fIn2, int[] fOut, int free, int mul) throws IOException {
        Schema s1 = new Schema.Parser().parse(new File(parentSchema));
        Schema s = new Schema.Parser().parse(new File(resultSchema));
        Schema s2 = new Schema.Parser().parse(new File(childSchema));

        List<Field> fs1 = s1.getFields();

        SortedAvroReader reader1 = new SortedAvroReader(parentPath, s1, fIn1);
        SortedAvroReader reader2 = new SortedAvroReader(childPath, s2, fIn2);

        SortedAvroWriter<ComparableKey, Record> writer = new SortedAvroWriter<ComparableKey, Record>(resultPath, s, free, mul);
        Record r2 = reader2.next();
        ComparableKey k2 = new ComparableKey(r2, fIn2);
        while (reader1.hasNext()) {
            Record r1 = reader1.next();
            Record data = new Record(s);
            for (int i = 0; i < fs1.size(); i++) {
                data.put(i, r1.get(i));
            }
            ComparableKey k1 = new ComparableKey(data, fIn1);
            List<Record> arr = new ArrayList<Record>();
            while (k2 != null && k1.compareTo(k2) == 0) {
                arr.add(r2);
//                System.out.println(r2);
                if (reader2.hasNext()) {
                    r2 = reader2.next();
                    k2 = new ComparableKey(r2, fIn2);
                } else {
                    k2 = null;
                    reader2.close();
                    break;
                }
            }
            data.put(fs1.size(), arr);
            writer.append(new ComparableKey(data, fOut), data);
        }
        reader1.close();
        writer.flush();
        System.gc();
    }

    public static void edgeSort(String edge, String schema, int needChange,
                         int[] edgeField,HashMap<Long, Integer> changeMap, int free, int mul, String resultPath) throws IOException {
        Schema edgeSchema = new Schema.Parser().parse(new File(schema));
        List<Field> fs = edgeSchema.getFields();
        SortedAvroWriter<ComparableKey, Record> writer = new SortedAvroWriter<ComparableKey, Record>(resultPath, edgeSchema,
                free, mul);
        BufferedReader edgeReader = new BufferedReader(new FileReader(new File(edge)));
        String line;
        while ((line = edgeReader.readLine()) != null) {
            String[] tmp = line.split("\\|");
            Record data = new Record(edgeSchema);
            for (int i = 0; i < fs.size(); i++) {
                switch (fs.get(i).schema().getType()) {
                    case INT:
                        int temp = Integer.parseInt(tmp[i].trim());
                        if(i == needChange){
                            temp = changeMap.get(temp);
                        }
                        data.put(i, temp);
                        break;
                    case LONG:
                        long temp1 = Long.parseLong(tmp[i].trim());
                        if(i == needChange){
                            temp1 = changeMap.get(temp1);
                        }
                        data.put(i, temp1);
                        break;
                    case FLOAT:
                        data.put(i, Float.parseFloat(tmp[i]));
                        break;
                    case DOUBLE:
                        data.put(i, Double.parseDouble(tmp[i]));
                        break;
                    case BYTES:
                        data.put(i, ByteBuffer.wrap(tmp[i].getBytes()));
                        break;
                    default:
                        data.put(i, tmp[i]);
                }

            }
            writer.append(new ComparableKey(data, edgeField), data);
        }
        edgeReader.close();
        writer.flush();
        System.gc();
    }

    public static void processEdge(String vertex, String vertexSchema, String edge, String edgeSchema, int needChange,
                                      int[] vertexFiled, int[] edgeField, String resultPath, int free, int mul) throws IOException {
        Schema v1 = new Schema.Parser().parse(new File(vertexSchema));
        SortedAvroReader vertex2Reader = new SortedAvroReader(vertex, v1, vertexFiled);
        vertex2Reader.create();
        HashMap<Long, Integer> idToColumnMap = new HashMap<>();
        int index = 0;
        while(vertex2Reader.hasNext()){
            Record r1 = vertex2Reader.next();
            idToColumnMap.put((Long) r1.get(0), index++);
        }
        System.out.println(index);
//        for(int i : idToColumnMap.keySet()){
//            System.out.println(i + ":" + idToColumnMap.get(i));
//        }
        vertex2Reader.close();
        edgeSort(edge, edgeSchema, needChange, edgeField, idToColumnMap, free, mul, resultPath);
//        doublePri(vertex, resultPath+"edge/", schema2, schema2 + "edge.avsc",
//                new int[]{0}, new int[]{0}, new int[]{0}, resultPath + "/", free, mul);

    }

    public static void addDestVertex(String edgePath, String edgeSchema, String vertexPath, String vertexSchema,
                                 String nestSchema, String resultPath, int[] edgeField, int free, int mul) throws IOException {
        Schema s1 = new Schema.Parser().parse(new File(edgeSchema));
        Schema s = new Schema.Parser().parse(new File(nestSchema));
        Schema s2 = new Schema.Parser().parse(new File(vertexSchema));
        List<Field> fs1 = s1.getFields();

        SortedAvroReader reader1 = new SortedAvroReader(edgePath, s1, edgeField);
        SortedAvroReader reader2 = new SortedAvroReader(vertexPath, s2, new int[]{0});

        SortedAvroWriter<ComparableKey, Record> writer = new SortedAvroWriter<ComparableKey, Record>(resultPath, s, free, mul);
//        Record r2 = reader2.next();
//        ComparableKey k2 = new ComparableKey(r2, fIn2);
        while (reader1.hasNext()) {
            Record r1 = reader1.next();
            Record data = new Record(s);
            for (int i = 0; i < fs1.size(); i++) {
                data.put(i, r1.get(i));
            }
//            ComparableKey k1 = new ComparableKey(data, fIn1);
            int index = 10;
            List<Record> arr = new ArrayList<>();
            if(reader1.hasNext()){
                //if reader1 still has records, we store 10 nodes in this record
                while(--index > 0){
                    if(reader2.hasNext()){
                        arr.add(reader2.next());
                    }
                }
            }else{
                while(reader2.hasNext()){
                    Record r2 = reader2.next();
                    arr.add(r2);
                }
            }



//            while (reader2.hasNext()) {
//                r2 = reader2.next();
//                index++;
//                arr.add(r2);
//            }
//            System.out.println(arr);
//            reader2.close();

            data.put(fs1.size(), arr);
            writer.append(new ComparableKey(data, edgeField), data);
        }
        reader2.close();
        reader1.close();
        writer.flush();
        System.gc();
    }

    public static int singleFinalTran(String filePath, String schema, String resultPath, int free, int mul) throws IOException {
        Schema s = new Schema.Parser().parse(new File(schema));
        BufferedReader reader = new BufferedReader(new FileReader(filePath));

        BatchAvroColumnWriter<Record> writer = new BatchAvroColumnWriter<>(s, resultPath, free, mul);
        String line;
        List<Field> fs1 = s.getFields();
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|");
            Record data = new Record(s);
            for (int i = 0; i < fs1.size(); i++) {
                switch (fs1.get(i).schema().getType()) {
                    case INT:
                        data.put(i, Integer.parseInt(tmp[i]));
                        break;
                    case LONG:
                        data.put(i, Long.parseLong(tmp[i]));
                        break;
                    case FLOAT:
                        data.put(i, Float.parseFloat(tmp[i]));
                        break;
                    case DOUBLE:
                        data.put(i, Double.parseDouble(tmp[i]));
                        break;
                    case BYTES:
                        data.put(i, ByteBuffer.wrap(tmp[i].getBytes()));
                        break;
                    default:
                        data.put(i, tmp[i]);
                }
            }
            writer.append(data);
        }
        reader.close();
        return writer.flush();
    }

    public static int finalTran(String path1, String path2, String schema1, String schema2, int[] fIn1, int[] fIn2,
            String resultPath, int free, int mul) throws IOException {
        int x = 0;

        Schema s1 = new Schema.Parser().parse(new File(schema1 + "single.avsc"));
        Schema s = new Schema.Parser().parse(new File(schema1 + "nest.avsc"));
        Schema s2 = new Schema.Parser().parse(new File(schema2));
        List<Field> fs1 = s1.getFields();

        BufferedReader reader1 = new BufferedReader(new FileReader(new File(path1)));
        SortedAvroReader reader2 = new SortedAvroReader(path2, s2, fIn2);

        BatchAvroColumnWriter<Record> writer = new BatchAvroColumnWriter<Record>(s, resultPath, free, mul);

        String line;
        Record r2 = reader2.next();
        ComparableKey k2 = new ComparableKey(r2, fIn2);
        while ((line = reader1.readLine()) != null) {
            String[] tmp = line.split("\\|");
            Record data = new Record(s);
            for (int i = 0; i < fs1.size(); i++) {
                switch (fs1.get(i).schema().getType()) {
                    case INT:
                        data.put(i, Integer.parseInt(tmp[i]));
                        break;
                    case LONG:
                        data.put(i, Long.parseLong(tmp[i]));
                        break;
                    case FLOAT:
                        data.put(i, Float.parseFloat(tmp[i]));
                        break;
                    case DOUBLE:
                        data.put(i, Double.parseDouble(tmp[i]));
                        break;
                    case BYTES:
                        data.put(i, ByteBuffer.wrap(tmp[i].getBytes()));
                        break;
                    default:
                        data.put(i, tmp[i]);
                }
            }
            ComparableKey k1 = new ComparableKey(data, fIn1);

            while (k2 != null && k1.compareTo(k2) > 0) {
                if (reader2.hasNext()) {
                    r2 = reader2.next();
                    k2 = new ComparableKey(r2, fIn2);
                } else {
                    k2 = null;
                    reader2.close();
                    break;
                }
            }
            List<Record> arr = new ArrayList<Record>();
            while (k2 != null && k1.compareTo(k2) == 0) {
                if (r2.get(2) == null) {
                    x++;
                    r2.put(0, null);
                    r2.put(1, null);
                }
                arr.add(r2);
                if (reader2.hasNext()) {
                    r2 = reader2.next();
                    k2 = new ComparableKey(r2, fIn2);
                } else {
                    k2 = null;
                    reader2.close();
                    break;
                }
            }
            data.put(fs1.size(), arr);
            writer.append(data);
            //            count++;
            //            if (count >= 20)
            //                break;
        }
        reader1.close();
        reader2.close();
        int index = writer.flush();
        System.out.println("########################the null ps number: " + x);
        return index;
    }

    public static int newFinalTran(String parentPath, String parentSchema, String childPath, String childSchema,
                                   String resultPath, String resultSchema, int[] fIn1, int[] fIn2, int free, int mul) throws IOException{
        int x = 0;

        Schema s1 = new Schema.Parser().parse(new File(parentSchema));
        Schema s = new Schema.Parser().parse(new File(resultSchema));
        Schema s2 = new Schema.Parser().parse(new File(childSchema));
        List<Field> fs1 = s1.getFields();

        SortedAvroReader reader1 = new SortedAvroReader(parentPath, s1, fIn1);
        SortedAvroReader reader2 = new SortedAvroReader(childPath, s2, fIn2);
        BatchAvroColumnWriter<Record> writer = new BatchAvroColumnWriter<Record>(s, resultPath, free, mul);
        Record r2 = reader2.next();
        ComparableKey k2 = new ComparableKey(r2, fIn2);
        while (reader1.hasNext()) {
            Record r1 = reader1.next();
            Record data = new Record(s);
            for (int i = 0; i < fs1.size(); i++) {
                data.put(i, r1.get(i));
            }
            ComparableKey k1 = new ComparableKey(data, fIn1);
            while (k2 != null && k1.compareTo(k2) > 0) {
                if (reader2.hasNext()) {
                    r2 = reader2.next();
                    k2 = new ComparableKey(r2, fIn2);
                } else {
                    k2 = null;
                    reader2.close();
                    break;
                }
            }
            List<Record> arr = new ArrayList<Record>();
            while (k2 != null && k1.compareTo(k2) == 0) {
                if (r2.get(2) == null) {
                    x++;
                    r2.put(0, null);
                    r2.put(1, null);
                }
                arr.add(r2);
                if (reader2.hasNext()) {
                    r2 = reader2.next();
                    k2 = new ComparableKey(r2, fIn2);
                } else {
                    k2 = null;
                    reader2.close();
                    break;
                }
            }
            data.put(fs1.size(), arr);
            writer.append(data);
        }

        int index = writer.flush();
        reader1.close();
        reader2.close();
        System.out.println("########################the null ps number: " + x);
        return index;
    }

    public static void avroRead(String filePath, String schema, int[] field, int num) throws IOException {
        Schema s = new Schema.Parser().parse(new File(schema));
        SortedAvroReader reader = new SortedAvroReader(filePath, s, field);
        Record r;
        int index = 0;
        File readerFile = new File("1.txt");
        FileWriter writer = new FileWriter(readerFile);

        while(reader.hasNext() && index < num){
            index++;
            r = reader.next();
            writer.write(r.toString());
            System.out.println(r);
        }
        writer.flush();
        writer.close();
        reader.close();
        System.gc();
    }

    public static void main(String[] args) throws IOException {
        String path = "T:\\TPCH\\";
        String path1 = "T:\\test_resources\\ppsl\\";
        String result = path1 + "result";
        String schema = path1 + "lay";
        int free = Integer.parseInt("5");
        int mul = Integer.parseInt("10");
        int max = Integer.parseInt("100");

//        int[] fields0 = new int[] { 1, 2, 0, 3 };
        long start;
        long end;
//        lSort(path + "lineitem.tbl", schema + "1/single.avsc", fields0, result + "1/", free, mul);
//        long end = System.currentTimeMillis();
//        System.out.println("+++++++lineitem sort time+++++++" + (end - start));

//        int[] fields1 = new int[] { 0, 1 };
//        int[] fields2 = new int[] { 1, 2 };
//        int[] fields3 = new int[] { 0, 1 };

//        start = System.currentTimeMillis();
//        lSort(path + "partsupp.tbl", schema + "2/single.avsc", fields1, result + "2/", free, mul);
//        end = System.currentTimeMillis();
//        System.out.println("+++++++partsupp sort time+++++++" + (end - start));
//
//        start = System.currentTimeMillis();
//        doublePri(result + "2/", result + "1/", schema + "2/", schema + "1/single.avsc", fields1, fields2, fields3,
//                result + "3/", free, mul);
//        end = System.currentTimeMillis();
//        System.out.println("+++++++partsupp&&lineitem time+++++++" + (end - start));
//
        int[] fields4 = new int[] { 0 };
        int[] fields5 = new int[] { 0 };
        start = System.currentTimeMillis();
        int index = finalTran(path + "part.tbl", result + "3/", schema + "3/", schema + "2/nest.avsc", fields4, fields5,
                result + "4/", max, mul);

        end = System.currentTimeMillis();
        System.out.println("+++++++part&&partsupp&&lineitem time+++++++" + (end - start));

        String resultPath = result + "4/";
        Schema s = new Schema.Parser().parse(new File(schema + "3/" + "nest.avsc"));
        BatchAvroColumnWriter<Record> writer = new BatchAvroColumnWriter<Record>(s, resultPath, max, mul);
        File[] files = new File[index];
        for (int i = 0; i < index; i++)
            files[i] = new File(resultPath + "file" + String.valueOf(i) + ".neci");
        if (index == 1) {
            new File(resultPath + "file0.head").renameTo(new File(resultPath + "result.head"));
            new File(resultPath + "file0.neci").renameTo(new File(resultPath + "result.neci"));
        } else {
            writer.mergeFiles(files);
        }
        System.out.println("merge completed!");
    }
}
