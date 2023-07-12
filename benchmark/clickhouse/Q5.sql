select person.p_id,
    person.p_fname,
    person.p_lname,
    person.p_credit_score,
    person.p_wallet_banlance
from person
    join person_click on person.p_id = person_click.p_id
    and person.p_credit_score >= 500
    and person.p_credit_score < 550
    and person.p_wallet_banlance >= 10000
    and person.p_wallet_banlance < 15000
    join clicks on clicks.cl_id = person_click.p_clid
    join campaign on clicks.cl_cid = campaign.c_id
    and (
        campaign.c_budget > 10000
        and campaign.c_budget < 15000
    )
    join wordset on campaign.c_id = wordset.w_cid
    join word on word.wo_wid = wordset.w_id
    and word.wo_word = 'secular'