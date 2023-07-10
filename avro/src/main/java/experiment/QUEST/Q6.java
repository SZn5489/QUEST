package experiment.QUEST;

import cores.avro.FilterOperator;
import experiment.filter.graph.CountryNameFilter;
import experiment.filter.graph.TagClassFilter;
import experiment.filter.relation.CreditFilter;
import experiment.filter.relation.WalletFilter;
import quest.DocDataFileReader;
import quest.GraphDataFileReader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Q6 {

    public static void main(String[] args) throws IOException {
        String relaPath = args[0];
        String graphPath = args[1];

        Schema relaSchema = new Schema.Parser().parse(new File(relaPath + "person.avsc"));

        Schema graphSchema = new Schema.Parser().parse(new File(graphPath + "nest.avsc"));

        File relaFile = new File(relaPath + "result/result.neci");

        File graphFile = new File(graphPath + "result/result.neci");
        String metaPath = args[2];




        FilterOperator[] relaFilters = new FilterOperator[2];
        relaFilters[0] = new CreditFilter(Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        relaFilters[1] = new WalletFilter(Integer.parseInt(args[5]), Integer.parseInt(args[6]));

        FilterOperator[] graphFilters = new FilterOperator[2];
        graphFilters[0] = new CountryNameFilter(args[7]);
        graphFilters[1] = new TagClassFilter(args[8]);

        ArrayList<String> vertexList = new ArrayList<>();
        HashMap<String, String> vertexToColumnIndex = new HashMap<>();
        ArrayList<String> path = new ArrayList<>();
        HashMap<String, String> columnIndexToVertex = new HashMap<>();
        ArrayList<String> columnIndexList = new ArrayList<>();


//        vertexList.add("root");
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

        path.add("country_name");
        path.add("root");
        path.add("tc_name");

        DocDataFileReader<GenericData.Record> relaReader = new DocDataFileReader<>(relaFile, relaFilters);
        relaReader.setMetaDir(metaPath + "/relation/");

        GraphDataFileReader<GenericData.Record> graphReader =
                new GraphDataFileReader<GenericData.Record>(graphFile, graphFilters, path, vertexList, columnIndexList, vertexToColumnIndex, columnIndexToVertex);
        graphReader.setMetaDir(metaPath + "/graph/");

        relaReader.createSchema(relaSchema);
        graphReader.createSchema(graphSchema);

        System.out.println("start filter......");
        long start = System.currentTimeMillis();
        long t1 = System.currentTimeMillis();
        relaReader.filter();
        graphReader.crossModel(relaReader, "p_id","p_id");

        graphReader.tranBitset("root");
        graphReader.tranBitset(graphFilters[0].getName());
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
        relaReader.close();
        long end = System.currentTimeMillis();
////        System.out.println(readSchema.getType());
        System.out.println("filter result: " + count);
        System.out.println("sum of data:" + sumC);
        System.out.println("time: " + (end - start));
        System.out.println("filter time: " + (t2 - t1));
        System.out.println("read time: " + (end - t2));
    }

}
