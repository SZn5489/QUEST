package experiment.filter.doc;

import cores.avro.FilterOperator;

public class BudgetFilter implements FilterOperator<Float> {

    float budget1;

    float budget2;

    public BudgetFilter(float budget1, float budget2){
        this.budget1 = budget1;
        this.budget2 = budget2;
    }

    @Override
    public String getName() {
        return "c_budget";
    }

    @Override
    public boolean isMatch(Float aFloat) {
        return budget1 <= aFloat && budget2 > aFloat;
    }
}
