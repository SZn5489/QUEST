package experiment.filter.relation;

import cores.avro.FilterOperator;

public class CreditFilter implements FilterOperator<Integer> {

    int score1;
    int score2;

    public CreditFilter(int score1, int score2){
        this.score1 = score1;
        this.score2 = score2;
    }

    @Override
    public String getName() {
        return "p_credit_score";
    }

    @Override
    public boolean isMatch(Integer integer) {
        return score1 <= integer && score2 > integer;
    }
}
