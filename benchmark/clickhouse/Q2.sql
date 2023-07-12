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
    and person.p_fname = 'Rodrigo'
    join person_likes_comment on person_ldbc.p_id = person_likes_comment.l_personId
    join comment on person_likes_comment.l_commentId = comment.c_id
    join comment_hasTag_tag on comment.c_id = comment_hasTag_tag.ht_commentId
    join tag on comment_hasTag_tag.ht_tagId = tag.t_id
    join tag_hasType_tagclass on tag.t_id = tag_hasType_tagclass.has_tagId
    join tagclass on tag_hasType_tagclass.has_tagClassId = tagclass.tc_id
    and tagclass.tc_name = 'Person'
    join person_click on person.p_id = person_click.p_id
    join clicks on clicks.cl_id = person_click.p_clid
    join campaign on clicks.cl_cid = campaign.c_id
    join wordset on campaign.c_id = wordset.w_cid
    join word on word.wo_wid = wordset.w_id
    and word.wo_word = 'secular'