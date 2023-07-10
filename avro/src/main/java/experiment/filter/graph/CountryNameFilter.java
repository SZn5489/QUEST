package experiment.filter.graph;

import cores.avro.FilterOperator;

public class CountryNameFilter implements FilterOperator<String> {

    String countryName;

    public CountryNameFilter(String name){
        this.countryName = name;
    }

    @Override
    public String getName() {
        return "country_name";
    }

    @Override
    public boolean isMatch(String s) {
        return this.countryName.compareTo(s)==0;
    }
}
