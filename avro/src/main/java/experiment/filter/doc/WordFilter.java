package experiment.filter.doc;

import cores.avro.FilterOperator;

public class WordFilter implements FilterOperator<String> {

    String word;

    public WordFilter(String word){
        this.word = word;
    }

    @Override
    public String getName() {
        return "wo_word";
    }

    @Override
    public boolean isMatch(String s) {
        return this.word.compareTo(s)==0;
    }
}
