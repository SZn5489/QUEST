package experiment.filter.graph;

import cores.avro.FilterOperator;

public class TagClassFilter implements FilterOperator<String> {

    String name;

    public TagClassFilter(String name){
        this.name = name;
    }

    @Override
    public String getName() {
        return "tc_name";
    }

    @Override
    public boolean isMatch(String s) {
        return s.compareTo(this.name) == 0;
    }
}
