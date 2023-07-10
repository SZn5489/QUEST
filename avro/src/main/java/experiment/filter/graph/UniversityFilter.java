package experiment.filter.graph;

import cores.avro.FilterOperator;

public class UniversityFilter implements FilterOperator<String> {

    String universityName;

    public UniversityFilter(String name){
        this.universityName = name;
    }
    @Override
    public String getName() {
        return "or_name";
    }

    @Override
    public boolean isMatch(String s) {
        return universityName.compareTo(s) == 0;
    }
}
