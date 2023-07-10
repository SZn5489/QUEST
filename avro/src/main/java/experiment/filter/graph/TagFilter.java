package experiment.filter.graph;

import cores.avro.FilterOperator;

public class TagFilter implements FilterOperator<String> {

    String tagName;

    public TagFilter(String tagName){
        this.tagName = tagName;
    }
    @Override
    public String getName() {
        return "t_name";
    }

    @Override
    public boolean isMatch(String s) {
        return tagName.compareTo(s) == 0;
    }
}
