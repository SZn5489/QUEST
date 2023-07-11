neo4j-admin database import full         \
    --nodes=Relational_Person="$NEO4J_CONTAINER_ROOT/import/person.csv" \
    --nodes=Doc_Advertiser="$NEO4J_CONTAINER_ROOT/import/document/advertiser.csv" \
    --nodes=Doc_Campaign="$NEO4J_CONTAINER_ROOT/import/document/campaign.csv" \
    --nodes=Doc_Click="$NEO4J_CONTAINER_ROOT/import/document/clicks.csv" \
    --nodes=Doc_Word="$NEO4J_CONTAINER_ROOT/import/document/word.csv" \
    --nodes=Doc_Wordset="$NEO4J_CONTAINER_ROOT/import/document/wordset.csv" \
    --relationships=CAMPAIGN_HAS_CREATOR_ADVERTISER="$NEO4J_CONTAINER_ROOT/import/document/campaign_hasCreator_advertiser.csv" \
    --relationships=CLICKS_BELONGSTO_CAMPAIGN="$NEO4J_CONTAINER_ROOT/import/document/clicks_belongsTo_campaign.csv" \
    --relationships=PERSON_MADE_CLICK="$NEO4J_CONTAINER_ROOT/import/document/person_click.csv" \
    --relationships=WORD_BELONGSTO_WORDSET="$NEO4J_CONTAINER_ROOT/import/document/word_belongsTo_wordset.csv" \
    --relationships=WORDSET_BELONGSTO_CAMPAIGN="$NEO4J_CONTAINER_ROOT/import/document/wordset_belongsTo_campaign.csv" \
    --nodes=Place="$NEO4J_CONTAINER_ROOT/import/static/place$NEO4J_CSV_POSTFIX" \
    --nodes=Organisation="$NEO4J_CONTAINER_ROOT/import/static/organisation$NEO4J_CSV_POSTFIX" \
    --nodes=TagClass="$NEO4J_CONTAINER_ROOT/import/static/tagclass$NEO4J_CSV_POSTFIX" \
    --nodes=Tag="$NEO4J_CONTAINER_ROOT/import/static/tag$NEO4J_CSV_POSTFIX" \
    --nodes=Comment:Message="$NEO4J_CONTAINER_ROOT/import/dynamic/comment$NEO4J_CSV_POSTFIX" \
    --nodes=Person="$NEO4J_CONTAINER_ROOT/import/dynamic/person$NEO4J_CSV_POSTFIX" \
    --relationships=IS_PART_OF="$NEO4J_CONTAINER_ROOT/import/static/place_isPartOf_place$NEO4J_CSV_POSTFIX" \
    --relationships=IS_LOCATED_IN="$NEO4J_CONTAINER_ROOT/import/static/organisation_isLocatedIn_place$NEO4J_CSV_POSTFIX" \
    --relationships=HAS_TYPE="$NEO4J_CONTAINER_ROOT/import/static/tag_hasType_tagclass$NEO4J_CSV_POSTFIX" \
    --relationships=HAS_INTEREST="$NEO4J_CONTAINER_ROOT/import/dynamic/person_hasInterest_tag$NEO4J_CSV_POSTFIX" \
    --relationships=KNOWS="$NEO4J_CONTAINER_ROOT/import/dynamic/person_knows_person$NEO4J_CSV_POSTFIX" \
    --relationships=LIKES="$NEO4J_CONTAINER_ROOT/import/dynamic/person_likes_comment$NEO4J_CSV_POSTFIX" \
    --relationships=HAS_TAG="$NEO4J_CONTAINER_ROOT/import/dynamic/comment_hasTag_tag$NEO4J_CSV_POSTFIX" \
    --relationships=STUDY_AT="$NEO4J_CONTAINER_ROOT/import/dynamic/person_studyAt_organisation$NEO4J_CSV_POSTFIX" \
    --delimiter '|'
