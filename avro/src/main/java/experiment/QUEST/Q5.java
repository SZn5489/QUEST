package experiment.QUEST;

import cores.avro.FilterOperator;
import experiment.filter.doc.BudgetFilter;
import experiment.filter.doc.WordFilter;
import experiment.filter.relation.CreditFilter;
import experiment.filter.relation.WalletFilter;
import quest.DocDataFileReader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.io.File;
import java.io.IOException;
public class Q5 {

    public static void main(String[] args) throws IOException {
        String docPath = args[0];
        String relaPath = args[1];

        Schema relaSchema = new Schema.Parser().parse(new File(relaPath + "person.avsc"));

        Schema docSchema = new Schema.Parser().parse(new File(docPath + "nest.avsc"));
        File docFile = new File(docPath + "result/result.neci");

        File relaFile = new File(relaPath + "result/result.neci");

        String metaPath = args[2];

        FilterOperator[] docFilters = new FilterOperator[2];
        docFilters[0] = new WordFilter(args[3]);
        docFilters[1] = new BudgetFilter(Float.parseFloat(args[4]), Float.parseFloat(args[5]));


        FilterOperator[] relaFilters = new FilterOperator[2];
        relaFilters[0] = new CreditFilter(Integer.parseInt(args[6]), Integer.parseInt(args[7]));
        relaFilters[1] = new WalletFilter(Integer.parseInt(args[8]), Integer.parseInt(args[9]));

        DocDataFileReader<GenericData.Record> relaReader = new DocDataFileReader<>(relaFile, relaFilters);
        relaReader.setMetaDir(metaPath + "/relation/");

        DocDataFileReader<GenericData.Record> docReader = new DocDataFileReader<>(docFile, docFilters);
        docReader.setMetaDir(metaPath + "/doc/");

        relaReader.createSchema(relaSchema);
        docReader.createSchema(docSchema);

        System.out.println("start filter......");
        long start = System.currentTimeMillis();
        long t1 = System.currentTimeMillis();
        docReader.filter();
        relaReader.crossModel(docReader, "p_id","p_id");
        relaReader.filter();

        long t2 = System.currentTimeMillis();
        System.out.println("filter successfully!");
        relaReader.createFilterRead(10000);
        int count = 0;
        int sumC = relaReader.getRowCount(0);
        while (relaReader.hasNext()) {
            GenericData.Record r = relaReader.next();
//            System.out.println(r);
//            System.out.println(r.toString());
            count++;
        }
        relaReader.close();
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
