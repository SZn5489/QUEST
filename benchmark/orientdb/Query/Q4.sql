SELECT FROM Person WHERE p_id IN
(SELECT p_id FROM Person_r WHERE p_id IN
(SELECT p_id FROM (SELECT EXPAND(out('CampaignList').out("ClickList").out("PersonClickList")) FROM Advertiser
WHERE out("CampaignList").out("WordSetList").out("WordList") CONTAINS (wo_word ='secular')))
AND p_credit_score BETWEEN 500 AND 550)
AND out('LikesComment').out('HasTag').out('HasType').tc_name CONTAINS 'Person'
AND out('StudyAt').out('IsLocatedIn').out('IsPartOf').co_name CONTAINS 'China'
AND out('Knows').out('HasInterest').out('HasType').tc_name CONTAINS 'Person'