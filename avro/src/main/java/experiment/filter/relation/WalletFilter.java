package experiment.filter.relation;

import cores.avro.FilterOperator;

public class WalletFilter implements FilterOperator<Integer> {

    int banlance1;
    int banlance2;

    public WalletFilter(int banlance1, int banlance2){
        this.banlance1 = banlance1;
        this.banlance2 = banlance2;
    }
    @Override
    public String getName() {
        return "p_wallet_banlance";
    }

    @Override
    public boolean isMatch(Integer integer) {
        return banlance1 <= integer && banlance2 > integer;
    }
}
