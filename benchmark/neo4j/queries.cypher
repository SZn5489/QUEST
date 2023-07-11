q1
match(person:Relational_Person)
where person.credit_score>=500 and person.credit_score<550 and person.wallet_balances>=10000 and person.wallet_balances<15000
with person.id as re_filtered_personIds
match(person:Person)
where person.id in re_filtered_personIds
with person
match(word:Doc_Word {word:"secular"})-[:WORD_BELONGSTO_WORDSET]->(wordset:Doc_Wordset)-[:WORDSET_BELONGSTO_CAMPAIGN]->(campaign:Doc_Campaign)<-[:CLICKS_BELONGSTO_CAMPAIGN]-(click:Doc_Click)<-[:PERSON_MADE_CLICK]-(person)
where campaign.budget>=10000 and campaign.budget<15000
with person
match(person)-[:LIKES]->(comment:Comment)-[:HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass {name:"Person"})
with person
match(person)-[:STUDY_AT]->(org:Organisation)-[:IS_LOCATED_IN]->(place1:Place)-[:IS_PART_OF*0..10]->(country:Country {name:"China"})
return person

q2
match(person:Relational_Person {first_name:"Lin"})
where person.credit_score>=500 and person.credit_score<550 and person.wallet_balances>=10000 and person.wallet_balances<15000
with person.id as re_filtered_personIds
match(person:Person)
where person.id in re_filtered_personIds
with person
match(word:Doc_Word {word:"secular"})-[:WORD_BELONGSTO_WORDSET]->(wordset:Doc_Wordset)-[:WORDSET_BELONGSTO_CAMPAIGN]->(campaign:Doc_Campaign)<-[:CLICKS_BELONGSTO_CAMPAIGN]-(click:Doc_Click)<-[:PERSON_MADE_CLICK]-(person)
with person
match(person)-[:LIKES]->(comment:Comment)-[:HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass {name:"Person"})
return person

q3
match(person:Relational_Person)
where person.credit_score>=500 and person.credit_score<550
with person.id as re_filtered_personIds
match(person:Person)
where person.id in re_filtered_personIds
with person
match(word:Doc_Word {word:"secular"})-[:WORD_BELONGSTO_WORDSET]->(wordset:Doc_Wordset)-[:WORDSET_BELONGSTO_CAMPAIGN]->(campaign:Doc_Campaign)<-[:CLICKS_BELONGSTO_CAMPAIGN]-(click:Doc_Click)<-[:PERSON_MADE_CLICK]-(person)
where campaign.budget>=10000 and campaign.budget<15000
with person,campaign
match(campaign)<-[:CLICKS_BELONGSTO_CAMPAIGN]-(click:Doc_Click)
where click.date>="2018-01-01 00:00:00" and click.date<"2020-01-01 00:00:00"
with person
match(person)-[:LIKES]->(comment:Comment)-[:HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass {name:"Person"})
return person

q4
match(person:Relational_Person)
where person.credit_score>=500 and person.credit_score<550 
with person.id as re_filtered_personIds
match(person:Person)
where person.id in re_filtered_personIds
with person
match(word:Doc_Word {word:"secular"})-[:WORD_BELONGSTO_WORDSET]->(wordset:Doc_Wordset)-[:WORDSET_BELONGSTO_CAMPAIGN]->(campaign:Doc_Campaign)<-[:CLICKS_BELONGSTO_CAMPAIGN]-(click:Doc_Click)<-[:PERSON_MADE_CLICK]-(person)
with person
match(tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass {name:"Person"})
with person,tag
match(person)-[:LIKES]->(comment:Comment)-[:HAS_TAG]->(tag)
with person,tag
match(person1)-[:KNOWS]->(person2:Person)-[:HAS_INTEREST]->(tag)
with person1 as person
match(person)-[:STUDY_AT]->(org:Organisation)-[:IS_LOCATED_IN]->(place:Place)-[:IS_PART_OF*0..10]->(country:Country {name:"China"})
return person


q5
match(person:Relational_Person)
where person.credit_score>=500 and person.credit_score<550 and person.wallet_balances>=10000 and person.wallet_balances<15000
with person.id as re_filtered_personIds
match(person:Person)
where person.id in re_filtered_personIds
with person
match(word:Doc_Word {word:"secular"})-[:WORD_BELONGSTO_WORDSET]->(wordset:Doc_Wordset)-[:WORDSET_BELONGSTO_CAMPAIGN]->(campaign:Doc_Campaign)<-[:CLICKS_BELONGSTO_CAMPAIGN]-(click:Doc_Click)<-[:PERSON_MADE_CLICK]-(person)
where campaign.budget>=10000 and campaign.budget<15000
return person
limit 1


q6
match(person:Relational_Person)
where person.credit_score>=500 and person.credit_score<550 and person.wallet_balances>=10000 and person.wallet_balances<15000
with person.id as re_filtered_personIds
match(person:Person)
where person.id in re_filtered_personIds
with person
match(person)-[:LIKES]->(comment:Comment)-[:HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass {name:"Person"})
with person
match(person)-[:STUDY_AT]->(org:Organisation)-[:IS_LOCATED_IN]->(place:Place)-[:IS_PART_OF*0..10]->(country:Country {name:"China"})
return person

q7
match(person:Person)
match(word:Doc_Word {word:"secular"})-[:WORD_BELONGSTO_WORDSET]->(wordset:Doc_Wordset)-[:WORDSET_BELONGSTO_CAMPAIGN]->(campaign:Doc_Campaign)<-[:CLICKS_BELONGSTO_CAMPAIGN]-(click:Doc_Click)<-[:PERSON_MADE_CLICK]-(person)
where campaign.budget>=10000 and campaign.budget<15000
with person
match(person)-[:LIKES]->(comment:Comment)-[:HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass {name:"Person"})
with person
match(person)-[:STUDY_AT]->(org:Organisation)-[:IS_LOCATED_IN]->(place:Place)-[:IS_PART_OF*0..10]->(country:Country {name:"China"})
return person


q8
match(person:Relational_Person)
where person.credit_score>=300 and person.credit_score<800 and person.wallet_balances>=5000 and person.wallet_balances<50000
with person.id as re_filtered_personIds
match(person:Person)
where person.id in re_filtered_personIds
with person
match(word:Doc_Word {word:"secular"})-[:WORD_BELONGSTO_WORDSET]->(wordset:Doc_Wordset)-[:WORDSET_BELONGSTO_CAMPAIGN]->(campaign:Doc_Campaign)<-[:CLICKS_BELONGSTO_CAMPAIGN]-(click:Doc_Click)<-[:PERSON_MADE_CLICK]-(person)
where campaign.budget>=5000 and campaign.budget<25000
with person
match(person)-[:LIKES]->(comment:Comment)-[:HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass {name:"Person"})
with person
match(person)-[:STUDY_AT]->(org:Organisation)-[:IS_LOCATED_IN]->(place1:Place)-[:IS_PART_OF*0..10]->(country:Country {name:"China"})
return person

q9

match(person:Relational_Person)
where person.credit_score>=500 and person.credit_score<550 and person.wallet_balances>=10000 and person.wallet_balances<15000
with person.id as re_filtered_personIds
match(person:Person)
where person.id in re_filtered_personIds
with person
match(person)-[:PERSON_MADE_CLICK]->(click:Doc_Click)
where click.date>="2018-01-01 00:00:00" and click.date<"2020-01-01 00:00:00" and click.fee>=5000 and click.fee<8000
with person
match(person)-[:LIKES]->(comment:Comment)-[:HAS_TAG]->(tag:Tag {name:"Time3"})
with person
match(person)-[:STUDY_AT]->(org:Organisation {name:"Central_University_of_Karnataka"} )
return person


q10

match(person:Person {id:"17592186134210"})
with person
match(word:Doc_Word {word:"secular"})-[:WORD_BELONGSTO_WORDSET]->(wordset:Doc_Wordset)-[:WORDSET_BELONGSTO_CAMPAIGN]->(campaign:Doc_Campaign)<-[:CLICKS_BELONGSTO_CAMPAIGN]-(click:Doc_Click)<-[:PERSON_MADE_CLICK]-(person)
where campaign.budget>=10000 and campaign.budget<15000
with campaign
match(campaign)-[:CAMPAIGN_HAS_CREATOR_ADVERTISER]->(adv:Doc_Advertiser)
return adv

q11

match(tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass {name:"Person"})
with tag
match(person:Person)-[:LIKES]->(comment:Comment)-[:HAS_TAG]->(tag)
with person,tag
match(person1)-[:KNOWS]->(person2:Person)-[:HAS_INTEREST]->(tag)
with person1 as person
match(person)-[:STUDY_AT]->(org:Organisation)-[:IS_LOCATED_IN]->(place:Place)-[:IS_PART_OF*0..10]->(country:Country {name:"China"})
return person




