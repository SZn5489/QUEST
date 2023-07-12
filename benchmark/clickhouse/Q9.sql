select person.p_id,
    person.p_fname,
    person.p_lname,
    person.p_credit_score,
    person.p_wallet_banlance
from person
    join person_ldbc on person.p_id = person_ldbc.p_id
    and person.p_credit_score >= 500
    and person.p_credit_score < 550
    and person.p_wallet_banlance >= 10000
    and person.p_wallet_banlance < 15000
    join person_hasInterest_tag on person_ldbc.p_id = person_hasInterest_tag.hi_personId
    join tag on person_hasInterest_tag.hi_tagId = tag.t_id
    and tag.t_name = 'Time3'
    join person_studyAt_organisation on person_studyAt_organisation.st_pid = person_ldbc.p_id
    join organisation on person_studyAt_organisation.st_organid = organisation.or_id
    and organisation.or_name = 'Shanghai Institute of Foreign Trade'
    join person_click on person.p_id = person_click.p_id
    join clicks on clicks.cl_id = person_click.p_clid
    and clicks.cl_fee >= 10000
    and clicks.cl_date >= '2018-01-01 00:00:00'