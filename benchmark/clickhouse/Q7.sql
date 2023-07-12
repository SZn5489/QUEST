with pl AS(
    select place.place_id
    from place
    where place_name = 'China'
)
select person_ldbc.p_id,
    person_ldbc.p_fname,
    person_ldbc.p_lname
from person_ldbc
    join person_likes_comment on person_ldbc.p_id = person_likes_comment.l_personId
    join comment on person_likes_comment.l_commentId = comment.c_id
    join comment_hasTag_tag on comment.c_id = comment_hasTag_tag.ht_commentId
    join tag on comment_hasTag_tag.ht_tagId = tag.t_id
    join tag_hasType_tagclass on tag.t_id = tag_hasType_tagclass.has_tagId
    join tagclass on tag_hasType_tagclass.has_tagClassId = tagclass.tc_id
    and tagclass.tc_name = 'Person'
    join person_studyAt_organisation on person_studyAt_organisation.st_pid = person_ldbc.p_id
    join organisation on person_studyAt_organisation.st_organid = organisation.or_id
    join organisation_isLocatedIn_place on organisation.or_id = organisation_isLocatedIn_place.is_organization_id
    join place on organisation_isLocatedIn_place.is_place_id = place.place_id
    join place_isPartOf_place on place_isPartOf_place.place_in_id = place.place_id
    join pl on pl.place_id = place_isPartOf_place.place_out_id
    join person_click on person_ldbc.p_id = person_click.p_id
    join clicks on clicks.cl_id = person_click.p_clid
    join campaign on clicks.cl_cid = campaign.c_id
    and (
        campaign.c_budget > 10000
        and campaign.c_budget < 15000
    )
    join wordset on campaign.c_id = wordset.w_cid
    join word on word.wo_wid = wordset.w_id
    and word.wo_word = 'secular'