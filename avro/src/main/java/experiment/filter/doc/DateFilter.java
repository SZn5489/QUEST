package experiment.filter.doc;

import cores.avro.FilterOperator;

public class DateFilter implements FilterOperator<String> {

    String date1;
    String date2;

    public DateFilter(String date1, String date2){
        this.date1 = date1;
        this.date2 = date2;
    }

    @Override
    public String getName() {
        return "cl_date";
    }

    @Override
    public boolean isMatch(String s) {
        return date1.compareTo(s) <=0 && date2.compareTo(s) > 0;
    }
}
