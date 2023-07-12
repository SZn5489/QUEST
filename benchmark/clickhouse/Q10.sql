select *
from advertiser
    join campaign on advertiser.a_id = campaign.c_aid
    and (
        campaign.c_budget > 10000
        and campaign.c_budget < 15000
    )
    join wordset on campaign.c_id = wordset.w_cid
    join word on word.wo_wid = wordset.w_id
    and word.wo_word = 'secular'
    join clicks on clicks.cl_cid = campaign.c_id
    join person_click on clicks.cl_id = person_click.p_clid
    and person_click.p_id = '17592186134210'