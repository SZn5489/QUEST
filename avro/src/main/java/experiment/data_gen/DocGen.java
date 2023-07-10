package experiment.data_gen;
import cores.avro.BatchAvroColumnWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.io.File;
import java.io.IOException;

public class DocGen {

    public static void main(String[] args) throws IOException {

        String filePath = args[0];
        String docPath = args[1];
        String singleSchemaPath = filePath + "/singleSchema/";
        String nestSchemaPath = filePath + "/nestSchema/";
        String singleResultPath = filePath + "/singleResult/";
        String nestResultPath = filePath + "/nestResult/";
        String finalResultPath = filePath + "/result/";

        int free = Integer.parseInt(args[2]);
        int mul = Integer.parseInt(args[3]);
        int max = Integer.parseInt(args[4]);


        DataTran.lSort(docPath + "advertiser.tbl", singleSchemaPath + "Advertiser.avsc", new int[]{0}, singleResultPath + "Advertiser/",free, mul);
        DataTran.lSort(docPath + "campaign.tbl", singleSchemaPath + "Campaign.avsc", new int[]{0}, singleResultPath + "Campaign/",free, mul);
        DataTran.lSort(docPath + "clicks.tbl", singleSchemaPath + "Click.avsc", new int[]{0}, singleResultPath + "Click/",free, mul);
        DataTran.lSort(docPath + "person_click.tbl", singleSchemaPath + "PersonClick.avsc", new int[]{1, 0}, singleResultPath + "PersonClick/",free, mul);

        DataTran.lSort(docPath + "wordset.tbl", singleSchemaPath + "WordSet.avsc", new int[]{0}, singleResultPath + "WordSet/",free, mul);
        DataTran.lSort(docPath + "word.tbl", singleSchemaPath + "Word.avsc", new int[]{0}, singleResultPath + "Word/",free, mul);

        DataTran.newDoublePri(singleResultPath + "WordSet/", singleSchemaPath + "WordSet.avsc",
                singleResultPath + "Word/", singleSchemaPath + "Word.avsc",
                nestResultPath + "WordSet_Word/", nestSchemaPath + "WordSet_Word.avsc", new int[]{0}, new int[]{0}, new int[]{1, 0}, free, mul);

        DataTran.newDoublePri(singleResultPath + "Campaign/", singleSchemaPath + "Campaign.avsc",
                nestResultPath + "WordSet_Word/", nestSchemaPath + "WordSet_Word.avsc",
                nestResultPath + "Campaign_WordSet/", nestSchemaPath + "Campaign_WordSet.avsc", new int[]{0}, new int[]{1, 0}, new int[]{0}, free, mul);

        DataTran.newDoublePri(singleResultPath + "Click/", singleSchemaPath + "Click.avsc",
                singleResultPath + "PersonClick/", singleSchemaPath + "PersonClick.avsc",
                nestResultPath + "Click_PersonClick/", nestSchemaPath + "Click_PersonClick.avsc", new int[]{0}, new int[]{1, 0}, new int[]{1, 0}, free, mul);

        DataTran.newDoublePri(nestResultPath + "Campaign_WordSet/", nestSchemaPath + "Campaign_WordSet.avsc",
                nestResultPath + "Click_PersonClick/", nestSchemaPath + "Click_PersonClick.avsc",
                nestResultPath + "Campaign_Click/", nestSchemaPath + "Campaign_Click.avsc", new int[]{0}, new int[]{1, 0}, new int[]{1, 0}, free, mul);

        int index = DataTran.newFinalTran(singleResultPath + "Advertiser/", singleSchemaPath + "Advertiser.avsc",
                nestResultPath + "Campaign_Click/", nestSchemaPath + "Campaign_Click.avsc",
                finalResultPath, nestSchemaPath + "Advertiser_Campaign.avsc", new int[]{0}, new int[]{1, 0}, free, mul);

        Schema s = new Schema.Parser().parse(new File(nestSchemaPath+"Advertiser_Campaign.avsc"));
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
