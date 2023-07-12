create table person_ldbc
(p_id UInt64, p_fname String, p_lname String, p_gender String, 
p_birthday String, p_creationDate String, p_locationIP String, 
p_browserUsed String)
ENGINE = MergeTree
PRIMARY KEY(p_id)

INSERT INTO person_ldbc
FROM INFILE '../origin_data/graph/csv/person_0_0.csv'
FORMAT CSV

create table person_hasInterest_tag
(hi_personId UInt64, hi_tagId UInt64)
ENGINE = MergeTree
PRIMARY KEY(hi_personId)

INSERT INTO person_hasInterest_tag
FROM INFILE '../origin_data/graph/csv/person_hasInterest_tag_0_0.csv'
FORMAT CSV

create table person_knows_person
(know_pid UInt64, know_kpid UInt64, know_creationDate String)
ENGINE = MergeTree
PRIMARY KEY(know_pid)

INSERT INTO person_knows_person
FROM INFILE '../origin_data/graph/csv/person_knows_person_0_0.csv'
FORMAT CSV

create table person_studyAt_organisation
(st_pid UInt64, st_organid UInt64, st_classYear UInt32)
ENGINE = MergeTree
PRIMARY KEY(st_pid)

INSERT INTO person_studyAt_organisation
FROM INFILE '../origin_data/graph/csv/person_studyAt_organisation_0_0.csv'
FORMAT CSV

create table organisation
(or_id UInt64, or_type String, or_name String, or_url String)
ENGINE = MergeTree
PRIMARY KEY(or_id)

INSERT INTO organisation
FROM INFILE '../origin_data/graph/csv/organisation_0_0.csv'
FORMAT CSV

create table organisation_isLocatedIn_place
(is_organization_id UInt64, is_place_id UInt64)
ENGINE = MergeTree
PRIMARY KEY(is_organization_id)

INSERT INTO organisation_isLocatedIn_place
FROM INFILE '../origin_data/graph/csv/organisation_isLocatedIn_place_0_0.csv'
FORMAT CSV

create table place
(place_id UInt64, place_name String, place_url String, place_type String)
ENGINE = MergeTree
PRIMARY KEY(place_id)

INSERT INTO place
FROM INFILE '../origin_data/graph/csv/place_0_0.csv'
FORMAT CSV

create table place_isPartOf_place
(place_in_id UInt64,  place_out_id UInt64,)
ENGINE = MergeTree
PRIMARY KEY(place_in_id)

INSERT INTO place_isPartOf_place
FROM INFILE '../origin_data/graph/csv/place_isPartOf_place_0_0.csv'
FORMAT CSV

create table person_likes_comment
(l_personId UInt64,  l_commentId UInt64, l_createDate String)
ENGINE = MergeTree
PRIMARY KEY(l_personId)

INSERT INTO person_likes_comment
FROM INFILE '../origin_data/graph/csv/person_likes_comment_0_0.csv'
FORMAT CSV


create table comment
(c_id UInt64,  c_creationDate String, c_browserUsed String, c_locationIp String,
c_content String, c_length UInt32)
ENGINE = MergeTree
PRIMARY KEY(c_id)

INSERT INTO comment
FROM INFILE '../origin_data/graph/csv/comment_0_0.csv'
FORMAT CSV

create table comment_hasTag_tag
(ht_commentId UInt64,  ht_tagId UInt64)
ENGINE = MergeTree
PRIMARY KEY(ht_commentId)

INSERT INTO comment_hasTag_tag
FROM INFILE '../origin_data/graph/csv/comment_hasTag_tag_0_0.csv'
FORMAT CSV

create table tag
(t_id UInt64,  t_name String, t_url String)
ENGINE = MergeTree
PRIMARY KEY(t_id)

INSERT INTO tag
FROM INFILE '../origin_data/graph/csv/tag_0_0.csv'
FORMAT CSV

create table tag_hasType_tagclass
(has_tagId UInt64,  has_tagClassId UInt64)
ENGINE = MergeTree
PRIMARY KEY(has_tagId)

INSERT INTO tag_hasType_tagclass
FROM INFILE '../origin_data/graph/csv/tag_hasType_tagclass_0_0.csv'
FORMAT CSV

create table tagclass
(tc_id UInt64,  tc_name String, tc_url String)
ENGINE = MergeTree
PRIMARY KEY(tc_id)

INSERT INTO tagclass
FROM INFILE '../origin_data/graph/csv/tagclass_0_0.csv'
FORMAT CSV



create table person
(p_id UInt64, p_fname String, p_lname String, p_credit_score UInt32, p_wallet_banlance UInt32)
ENGINE = MergeTree
PRIMARY KEY(p_id)

INSERT INTO person
FROM INFILE '../origin_data/relation/person.csv'
FORMAT CSV

##document
create table advertiser
(a_id UInt64, a_email String, a_name String)
ENGINE = MergeTree
PRIMARY KEY(a_id)

INSERT INTO advertiser
FROM INFILE '../origin_data/doc/advertiser.csv'
FORMAT CSV

create table campaign
(c_id UInt64, c_aid UInt64, c_budget Float32)
ENGINE = MergeTree
PRIMARY KEY(c_id)

INSERT INTO campaign
FROM INFILE '../origin_data/doc/campaign.csv'
FORMAT CSV

create table clicks
(cl_id UInt64, cl_cid UInt64, cl_fee Float32, cl_date String)
ENGINE = MergeTree
PRIMARY KEY(cl_id)

INSERT INTO clicks
FROM INFILE '../origin_data/doc/clicks.csv'
FORMAT CSV

create table wordset
(w_id UInt64, w_cid UInt64)
ENGINE = MergeTree
PRIMARY KEY(w_id)

INSERT INTO wordset
FROM INFILE '../origin_data/doc/wordset.csv'
FORMAT CSV

create table word
(wo_wid UInt64, wo_word String)
ENGINE = MergeTree
PRIMARY KEY(wo_wid)

INSERT INTO word
FROM INFILE '../origin_data/doc/word.csv'
FORMAT CSV

create table person_click
(p_id UInt64, p_clid UInt64, p_clickDate String)
ENGINE = MergeTree
PRIMARY KEY(p_id)

INSERT INTO person_click
FROM INFILE '../origin_data/doc/person_click.csv'
FORMAT CSV