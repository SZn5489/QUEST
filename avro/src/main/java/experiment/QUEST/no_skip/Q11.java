package experiment.QUEST.no_skip;

import cores.avro.FilterOperator;
import experiment.filter.graph.CountryNameFilter;
import experiment.filter.graph.TagClassFilter;
import quest.GraphNoSkipFileReader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Q11 {

    public static void main(String[] args) throws IOException, InterruptedException {
        String graphPath = args[0];

        Schema graphSchema = new Schema.Parser().parse(new File(graphPath + "nest.avsc"));
        File graphFile = new File(graphPath + "result/result.neci");

        FilterOperator[] graphFilters = new FilterOperator[2];
        graphFilters[1] = new CountryNameFilter(args[1]);
        graphFilters[0] = new TagClassFilter(args[2]);
//        graphFilters[0] = new TagClassFilter(args[3]);

        FilterOperator[] graphFilters1 = new FilterOperator[1];
        graphFilters1[0] = new TagClassFilter(args[3]);


        ArrayList<String> vertexList = new ArrayList<>();
        HashMap<String, String> vertexToColumnIndex = new HashMap<>();
        ArrayList<String> path = new ArrayList<>();
        HashMap<String, String> columnIndexToVertex = new HashMap<>();
        ArrayList<String> columnIndexList = new ArrayList<>();


        vertexList.add("UniversityList[]");
        vertexList.add("CityList[]");
        vertexList.add("CountryList[]");
        vertexList.add("CommentList[]");
        vertexList.add("TagList[]");
        vertexList.add("TagClassList[]");

        vertexToColumnIndex.put("UniversityList[]", "st_organid");
        vertexToColumnIndex.put("CityList[]", "is_place_id");
        vertexToColumnIndex.put("CountryList[]", "is_countryId");
        vertexToColumnIndex.put("CommentList[]", "l_commentId");
        vertexToColumnIndex.put("TagList[]", "ht_tagId");
        vertexToColumnIndex.put("TagClassList[]", "has_tagClassId");

        columnIndexToVertex.put("st_organid", "UniversityList[]");
        columnIndexToVertex.put("is_place_id", "CityList[]");
        columnIndexToVertex.put("is_countryId", "CountryList[]");
        columnIndexToVertex.put("l_commentId", "CommentList[]");
        columnIndexToVertex.put("ht_tagId", "TagList[]");
        columnIndexToVertex.put("has_tagClassId", "TagClassList[]");
        columnIndexToVertex.put("hi_tagId", "TagList[]");
        columnIndexToVertex.put("know_kpid", "root");

        columnIndexList.add("st_organid");
        columnIndexList.add("is_place_id");
        columnIndexList.add("is_countryId");
        columnIndexList.add("l_commentId");
        columnIndexList.add("ht_tagId");
        columnIndexList.add("has_tagClassId");
        columnIndexList.add("hi_tagId");
        columnIndexList.add("know_kpid");


//        path.add("tc_name");
//        path.add("root");
//        path.add("know_kpid");
//        path.add("KnowsList[]");
//        path.add("root");
        path.add("tc_name");
        path.add("root");
        path.add("country_name");



        ArrayList<String> path1 = new ArrayList<>();
        path1.add("tc_name");
        GraphNoSkipFileReader<GenericData.Record> graphReader =
                new GraphNoSkipFileReader<GenericData.Record>(graphFile, graphFilters, path, vertexList, columnIndexList, vertexToColumnIndex, columnIndexToVertex);

        GraphNoSkipFileReader<GenericData.Record> graphReader1 =
                new GraphNoSkipFileReader<GenericData.Record>(graphFile, graphFilters1, path1, vertexList, columnIndexList, vertexToColumnIndex, columnIndexToVertex);

        graphReader.createSchema(graphSchema);
        graphReader1.createSchema(graphSchema);

        System.out.println("start filter......");
        long start = System.currentTimeMillis();
        long t1 = System.currentTimeMillis();
        graphReader1.filter();
        graphReader1.tranBitset("root");
        graphReader1.tranBitset("know_kpid");
        graphReader1.tranBitset("KnowsList[]");
        graphReader1.tranBitset("root");
        graphReader1.tranBitset("p_id");
        graphReader.crossModel(graphReader1, "p_id", "p_id", false);
        graphReader.tranBitset("root");
        graphReader.filter();
        graphReader.tranBitset("root");
        graphReader.tranBitset("p_id");
        long t2 = System.currentTimeMillis();
        System.out.println("filter successfully!");
        graphReader.createFilterRead(10000);
        int count = 0;
        int sumC = graphReader.getRowCount(0);
        while (graphReader.hasNext()) {
            GenericData.Record r = graphReader.next();
//            System.out.println(r);
//            System.out.println(r.toString());
            count++;
        }
        graphReader.close();
        long end = System.currentTimeMillis();
////        System.out.println(readSchema.getType());
        System.out.println("filter result: " + count);
        System.out.println("sum of data:" + sumC);
        System.out.println("time: " + (end - start));
        System.out.println("filter time: " + (t2 - t1));
        System.out.println("read time: " + (end - t2));

//        Runtime.getRuntime().exec(new String[]{"cat /proc/`ps -ef | grep NoQ11.jar|grep -v 'grep'|awk '{print $2}'`/status | grep \"VmPeak\\|VmHWM\""});
//        ProcessBuilder processBuilder = new ProcessBuilder("bash", "-c", "cat /proc/`ps -ef | grep NoQ11.jar|grep -v 'grep'|awk '{print $2}'`/status | grep \"VmPeak\\|VmHWM\"");
//        Process process = processBuilder.start();
    }

}
