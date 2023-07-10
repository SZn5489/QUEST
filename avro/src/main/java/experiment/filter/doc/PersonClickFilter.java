package experiment.filter.doc;

import cores.avro.FilterOperator;

public class PersonClickFilter implements FilterOperator<Long> {

    public PersonClickFilter(){

    }
    @Override
    public String getName() {
        return "p_id";
    }

    @Override
    public boolean isMatch(Long aLong) {
        return true;
    }
}
