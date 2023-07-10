package experiment.filter.doc;

import cores.avro.FilterOperator;

public class FeeFilter implements FilterOperator<Float> {

    float fee1, fee2;

    public FeeFilter(float fee1, float fee2){
        this.fee1 = fee1;
        this.fee2 = fee2;
    }

    @Override
    public String getName() {
        return "cl_fee";
    }

    @Override
    public boolean isMatch(Float aFloat) {
        return fee1 <= aFloat && fee2 > aFloat;
    }
}
