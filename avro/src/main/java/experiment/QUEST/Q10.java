package experiment.QUEST;

import cores.avro.FilterOperator;
import experiment.filter.doc.BudgetFilter;
import experiment.filter.doc.WordFilter;
import experiment.filter.graph.PersonIdFilter;
import quest.DocDataFileReader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.io.File;
import java.io.IOException;

public class Q10 {

    public static void main(String[] args) throws IOException, InterruptedException {
        String docPath = args[0];
        Schema docSchema = new Schema.Parser().parse(new File(docPath + "nest.avsc"));
        File docFile = new File(docPath + "result/result.neci");
        String metaPath = args[1];

        FilterOperator[] filters = new FilterOperator[3];
        filters[0] = new WordFilter(args[2]);
        filters[1] = new BudgetFilter(Float.parseFloat(args[3]), Float.parseFloat(args[4]));
        filters[2] = new PersonIdFilter(Long.parseLong(args[5]));

        DocDataFileReader<GenericData.Record> docReader = new DocDataFileReader<>(docFile, filters);
        docReader.setMetaDir(metaPath + "doc/");
        docReader.createSchema(docSchema);

        System.out.println("start filter......");
        long start = System.currentTimeMillis();
        long t1 = System.currentTimeMillis();
        docReader.filter();
        long t2 = System.currentTimeMillis();
        System.out.println("filter successfully!");
        docReader.createFilterRead(10000);
        int count = 0;
        int sumC = docReader.getRowCount(0);
        while (docReader.hasNext()) {
            GenericData.Record r = docReader.next();
//            System.out.println(r);
//            System.out.println(r.toString());
            count++;
        }
        docReader.close();
        long end = System.currentTimeMillis();
////        System.out.println(readSchema.getType());
        System.out.println("filter result: " + count);
        System.out.println("sum of data:" + sumC);
        System.out.println("time: " + (end - start));
        System.out.println("filter time: " + (t2 - t1));
        System.out.println("read time: " + (end - t2));


    }

}
