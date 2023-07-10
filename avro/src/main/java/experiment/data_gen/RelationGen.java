package experiment.data_gen;

import cores.avro.BatchAvroColumnWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.io.File;
import java.io.IOException;

public class RelationGen {

    public static void main(String[] args) throws IOException {
        String docPath = args[0];
        String schemaPath = args[1];
        String resultPath = args[2];
        int free = Integer.parseInt(args[3]);
        int mul = Integer.parseInt(args[4]);
        int index = DataTran.singleFinalTran(docPath, schemaPath, resultPath, free, mul);

        Schema s = new Schema.Parser().parse(new File(schemaPath));
        BatchAvroColumnWriter<GenericData.Record> writer = new BatchAvroColumnWriter<GenericData.Record>(s, resultPath, mul, mul);
        File[] files = new File[index];
        for (int i = 0; i < index; i++)
            files[i] = new File(resultPath + "file" + i + ".neci");
        if (index == 1) {
            new File(resultPath + "file0.head").renameTo(new File(resultPath + "result.head"));
            new File(resultPath + "file0.neci").renameTo(new File(resultPath + "result.neci"));
        } else {
            writer.mergeFiles(files);
        }
        System.out.println("merge completed!");

    }

}
