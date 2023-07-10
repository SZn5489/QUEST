package experiment.filter.relation;

import cores.avro.FilterOperator;

public class RFnameFilter implements FilterOperator<String> {

    String fname;

    public RFnameFilter(String fname){
        this.fname = fname;
    }

    @Override
    public String getName() {
        return "p_fname";
    }

    @Override
    public boolean isMatch(String s) {
        return fname.compareTo(s) == 0;
    }
}
