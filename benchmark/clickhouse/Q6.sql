with pl AS(
  select place.place_id
  from place
  where place_name = 'China'
)
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