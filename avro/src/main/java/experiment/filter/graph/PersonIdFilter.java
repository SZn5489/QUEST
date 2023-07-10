package experiment.filter.graph;

import cores.avro.FilterOperator;

public class PersonIdFilter implements FilterOperator<Long> {

    long pid;

    public PersonIdFilter(long pid){
        this.pid = pid;
    }

    @Override
    public String getName() {
        return "p_id";
    }

    @Override
    public boolean isMatch(Long aLong) {
        return this.pid == aLong;
    }
}
