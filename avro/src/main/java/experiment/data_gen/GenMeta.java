package experiment.data_gen;

import cores.avro.FilterOperator;
import quest.DocDataFileReader;
import quest.GraphDataFileReader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class GenMeta {

    public static void main(String[] args) throws IOException {

        String docPath = args[0];
        String relaPath = args[1];
        String graphPath = args[2];

        Schema relaSchema = new Schema.Parser().parse(new File(relaPath + "person.avsc"));

        Schema graphSchema = new Schema.Parser().parse(new File(graphPath + "nest.avsc"));

        Schema docSchema = new Schema.Parser().parse(new File(docPath + "nest.avsc"));
        File docFile = new File(docPath + "result/result.neci");

        File relaFile = new File(relaPath + "result/result.neci");

        File graphFile = new File(graphPath + "result/result.neci");
        String metaPath = args[3];

        FilterOperator[] relaFilters = new FilterOperator[0];
        FilterOperator[] docFilters = new FilterOperator[0];
        FilterOperator[] graphFilters = new FilterOperator[0];

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


        path.add("country_name");
        path.add("root");
        path.add("tc_name");

        DocDataFileReader<GenericData.Record> relaReader = new DocDataFileReader<>(relaFile, relaFilters);
        relaReader.setMetaDir(metaPath + "/relation/");

        DocDataFileReader<GenericData.Record> docReader = new DocDataFileReader<>(docFile, docFilters);
        docReader.setMetaDir(metaPath + "/doc/");

        GraphDataFileReader<GenericData.Record> graphReader =
                new GraphDataFileReader<GenericData.Record>(graphFile, graphFilters, path, vertexList, columnIndexList, vertexToColumnIndex, columnIndexToVertex);
        graphReader.setMetaDir(metaPath + "/graph/");

        relaReader.createSchema(relaSchema);
        System.out.println("relation metadata gen success!");
        docReader.createSchema(docSchema);
        System.out.println("document metadata gen success!");
        graphReader.createSchema(graphSchema, true);
        System.out.println("graph metadata gen success!");
    }

}
