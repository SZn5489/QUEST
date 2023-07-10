package experiment.data_gen;

import cores.avro.BatchAvroColumnWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.io.File;
import java.io.IOException;

public class GraphGen {
    public static void main(String[] args) throws IOException {

        String filePath = args[0];
        String docPath = args[1];
        String singleSchemaPath = filePath + "/singleSchema/";
        String nestSchemaPath = filePath + "/nestSchema/";
        String singleEdgeResultPath = filePath + "/singleEdgeSortResult/";
        String singleVertexResultPath = filePath + "/singleVertexSortResult/";
        String nestResultPath = filePath + "/nestResult/";
        String finalResultPath = filePath + "/result/";
//
        int free = Integer.parseInt(args[2]);
        int mul = Integer.parseInt(args[3]);
        int max = Integer.parseInt(args[4]);

        DataTran.lSort(docPath+"tagclass.tbl", singleSchemaPath + "TagClass.avsc",
                new int[]{0}, singleVertexResultPath + "TagClass/", free, mul);
        DataTran.lSort(docPath+"tag.tbl", singleSchemaPath +"Tag.avsc",
                new int[]{0}, singleVertexResultPath + "Tag/", free, mul);
        DataTran.lSort(docPath + "comment.tbl", singleSchemaPath + "Comment.avsc",
                new int[]{0}, singleVertexResultPath + "Comment/", free, mul);
        DataTran.lSort(docPath + "city.tbl", singleSchemaPath + "City.avsc",
                new int[]{0}, singleVertexResultPath + "City/", free, mul);
        DataTran.lSort(docPath + "country.tbl", singleSchemaPath + "Country.avsc",
                new int[]{0}, singleVertexResultPath + "Country/", free, mul);
        DataTran.lSort(docPath + "person.tbl", singleSchemaPath + "Person.avsc",
                new int[]{0}, singleVertexResultPath + "Person/", free, mul);
        DataTran.lSort(docPath + "university.tbl", singleSchemaPath + "University.avsc",
                new int[]{0}, singleVertexResultPath + "University/", free, mul);

        System.out.println("process edge: tag_hasType_tagclass");
        DataTran.processEdge(singleVertexResultPath +"TagClass/", singleSchemaPath + "TagClass.avsc",
                docPath + "tag_hasType_tagclass.tbl", singleSchemaPath + "tag_hasType_tagclass.avsc",
                1, new int[]{0}, new int[]{0, 1},
                singleEdgeResultPath + "tag_hasType_tagclass/", free, mul);

        System.out.println("process edge: comment_hasTag_tag");
        DataTran.processEdge(singleVertexResultPath + "Tag/", singleSchemaPath + "Tag.avsc",
                docPath + "comment_hasTag_tag.tbl", singleSchemaPath + "comment_hasTag_tag.avsc",
                1, new int[]{0}, new int[]{0, 1},
                singleEdgeResultPath + "comment_hasTag_tag/", free, mul);

        System.out.println("process edge: city_isPartOf_country");
        DataTran.processEdge(singleVertexResultPath + "Country/", singleSchemaPath + "Country.avsc",
                docPath + "city_isPartOf_country.tbl", singleSchemaPath + "city_isPartOf_country.avsc",
                1, new int[]{0}, new int[]{0, 1},
                singleEdgeResultPath + "city_isPartOf_country/", free, mul);

        System.out.println("process edge: person_hasInterest_tag");
        DataTran.processEdge(singleVertexResultPath + "Tag/", singleSchemaPath + "Tag.avsc",
                docPath + "person_hasInterest_tag.tbl", singleSchemaPath + "person_hasInterest_tag.avsc",
                1, new int[]{0}, new int[]{0, 1},
                singleEdgeResultPath + "person_hasInterest_tag/", free, mul);

        System.out.println("process edge: person_knows_person");
        DataTran.processEdge(singleVertexResultPath + "Person/", singleSchemaPath + "Person.avsc",
                docPath + "person_knows_person.tbl", singleSchemaPath + "person_knows_person.avsc",
                1, new int[]{0}, new int[]{0, 1},
                singleEdgeResultPath + "person_knows_person/", free, mul);

        System.out.println("process edge: person_likes_comment");
        DataTran.processEdge(singleVertexResultPath + "Comment/", singleSchemaPath + "Comment.avsc",
                docPath + "person_likes_comment.tbl", singleSchemaPath + "person_likes_comment.avsc",
                1, new int[]{0}, new int[]{0, 1},
                singleEdgeResultPath + "person_likes_comment/", free, mul);

        System.out.println("process edge: person_studyAt_university");
        DataTran.processEdge(singleVertexResultPath + "University/", singleSchemaPath + "University.avsc",
                docPath + "person_studyAt_university.tbl", singleSchemaPath + "person_studyAt_university.avsc",
                1, new int[]{0}, new int[]{0, 1},
                singleEdgeResultPath + "person_studyAt_university/", free, mul);

        System.out.println("process edge: university_isLocatedIn_city");
        DataTran.processEdge(singleVertexResultPath + "City/", singleSchemaPath + "City.avsc",
                docPath + "university_isLocatedIn_city.tbl", singleSchemaPath + "university_isLocatedIn_city.avsc",
                1, new int[]{0}, new int[]{0, 1},
                singleEdgeResultPath + "university_isLocatedIn_city/", free, mul);

        System.out.println("edge process successful!");

        System.out.println("1");
        DataTran.addDestVertex(singleEdgeResultPath + "tag_hasType_tagclass/", singleSchemaPath + "tag_hasType_tagclass.avsc",
                singleVertexResultPath+"TagClass/", singleSchemaPath+"TagClass.avsc",
                nestSchemaPath+"HasType_TagClass.avsc", nestResultPath+"HasType_TagClass/", new int[]{0, 1}, free, mul);
        System.out.println("2");
        DataTran.newDoublePri(singleVertexResultPath + "Tag/", singleSchemaPath+"Tag.avsc",
                nestResultPath+"HasType_TagClass/", nestSchemaPath+"HasType_TagClass.avsc",
                nestResultPath + "Tag_HasType/", nestSchemaPath + "Tag_HasType.avsc",
                new int[]{0}, new int[]{0, 1}, new int[]{0}, free, mul);
        System.out.println("3");
        DataTran.addDestVertex(singleEdgeResultPath + "comment_hasTag_tag/", singleSchemaPath + "comment_hasTag_tag.avsc",
                nestResultPath + "Tag_HasType/", nestSchemaPath + "Tag_HasType.avsc",
                nestSchemaPath+"HasTag_Tag.avsc", nestResultPath+"HasTag_Tag/", new int[]{0, 1}, free, mul);
        System.out.println("4");
        DataTran.newDoublePri(singleVertexResultPath + "Comment/", singleSchemaPath+"Comment.avsc",
                nestResultPath+"HasTag_Tag/", nestSchemaPath+"HasTag_Tag.avsc",
                nestResultPath + "Comment_HasTag/", nestSchemaPath + "Comment_HasTag.avsc",
                new int[]{0}, new int[]{0, 1}, new int[]{0}, free, mul);
        System.out.println("5");
        DataTran.addDestVertex(singleEdgeResultPath + "person_likes_comment/", singleSchemaPath + "person_likes_comment.avsc",
                nestResultPath + "Comment_HasTag/", nestSchemaPath + "Comment_HasTag.avsc",
                nestSchemaPath+"Likes_Comment.avsc", nestResultPath+"Likes_Comment/", new int[]{0, 1}, free, mul);

        System.out.println(6);
        DataTran.addDestVertex(singleEdgeResultPath + "city_isPartOf_country/", singleSchemaPath + "city_isPartOf_country.avsc",
                singleVertexResultPath + "Country/", singleSchemaPath + "Country.avsc",
                nestSchemaPath+"IsPartOf_Country.avsc", nestResultPath+"IsPartOf_Country/", new int[]{0, 1}, free, mul);

        System.out.println(7);
        DataTran.newDoublePri(singleVertexResultPath + "City/", singleSchemaPath+"City.avsc",
                nestResultPath+"IsPartOf_Country/", nestSchemaPath+"IsPartOf_Country.avsc",
                nestResultPath + "City_IsPartOf/", nestSchemaPath + "City_IsPartOf.avsc",
                new int[]{0}, new int[]{0, 1}, new int[]{0}, free, mul);

        System.out.println(8);
        DataTran.addDestVertex(singleEdgeResultPath + "university_isLocatedIn_city/", singleSchemaPath + "university_isLocatedIn_city.avsc",
                nestResultPath + "City_IsPartOf/", nestSchemaPath + "City_IsPartOf.avsc",
                nestSchemaPath+"IsLocatedIn_City.avsc", nestResultPath+"IsLocatedIn_City/", new int[]{0, 1}, free, mul);

        System.out.println(9);
        DataTran.newDoublePri(singleVertexResultPath + "University/", singleSchemaPath+"University.avsc",
                nestResultPath+"IsLocatedIn_City/", nestSchemaPath+"IsLocatedIn_City.avsc",
                nestResultPath + "University_IsLocatedIn/", nestSchemaPath + "University_IsLocatedIn.avsc",
                new int[]{0}, new int[]{0, 1}, new int[]{0}, free, mul);

        System.out.println(10);
        DataTran.addDestVertex(singleEdgeResultPath + "person_studyAt_university/", singleSchemaPath + "person_studyAt_university.avsc",
                nestResultPath + "University_IsLocatedIn/", nestSchemaPath + "University_IsLocatedIn.avsc",
                nestSchemaPath+"StudyAt_University.avsc", nestResultPath+"StudyAt_University/", new int[]{0, 1}, free, mul);

        System.out.println(11);
        DataTran.newDoublePri(singleVertexResultPath + "Person/", singleSchemaPath+"Person.avsc",
                singleEdgeResultPath+"person_hasInterest_tag/", singleSchemaPath+"person_hasInterest_tag.avsc",
                nestResultPath + "Person_HasInterest/", nestSchemaPath + "Person_HasInterest.avsc",
                new int[]{0}, new int[]{0, 1}, new int[]{0}, free, mul);
        System.out.println(12);
        DataTran.newDoublePri(nestResultPath + "Person_HasInterest/", nestSchemaPath+"Person_HasInterest.avsc",
                singleEdgeResultPath+"person_knows_person/", singleSchemaPath+"person_knows_person.avsc",
                nestResultPath + "Person_Knows/", nestSchemaPath + "Person_Knows.avsc",
                new int[]{0}, new int[]{0, 1}, new int[]{0}, free, mul);

        System.out.println(13);
        DataTran.newDoublePri(nestResultPath + "Person_Knows/", nestSchemaPath+"Person_Knows.avsc",
                nestResultPath+"StudyAt_University/", nestSchemaPath+"StudyAt_University.avsc",
                nestResultPath + "Person_StudyAt/", nestSchemaPath + "Person_StudyAt.avsc",
                new int[]{0}, new int[]{0, 1}, new int[]{0}, free, mul);

        System.out.println(14);
        int index = DataTran.newFinalTran(nestResultPath + "Person_StudyAt/", nestSchemaPath + "Person_StudyAt.avsc",
                nestResultPath + "Likes_Comment", nestSchemaPath + "Likes_Comment.avsc",
                finalResultPath,nestSchemaPath + "Person_Likes.avsc", new int[]{0}, new int[]{0},free, mul);

        Schema s = new Schema.Parser().parse(new File(nestSchemaPath+"Person_Likes.avsc"));
        BatchAvroColumnWriter<GenericData.Record> writer = new BatchAvroColumnWriter<GenericData.Record>(s, finalResultPath, max, mul);
        File[] files = new File[index];
        for (int i = 0; i < index; i++)
            files[i] = new File(finalResultPath + "file" + i + ".neci");
        if (index == 1) {
            new File(finalResultPath + "file0.head").renameTo(new File(finalResultPath + "result.head"));
            new File(finalResultPath + "file0.neci").renameTo(new File(finalResultPath + "result.neci"));
        } else {
            writer.mergeFiles(files);
        }
        System.out.println("merge completed!");
    }
}
