SELECT FROM Person WHERE p_id IN
(SELECT p_id FROM Person_r WHERE p_id IN
(SELECT p_id FROM (SELECT EXPAND(out('CampaignList').out("ClickList").out("PersonClickList")) FROM Advertiser
WHERE out("CampaignList").out("WordSetList").out("WordList") CONTAINS (wo_word ='secular')
AND out('CampaignList') CONTAINS (c_budget BETWEEN 5000 AND 25000)))
AND p_credit_score BETWEEN 300 AND 800
AND p_wallet_banlance BETWEEN 5000 AND 50000)
AND out('StudyAt').out('IsLocatedIn').out('IsPartOf').co_name CONTAINS 'China'
AND out('LikesComment').out('HasTag').out('HasType').tc_name CONTAINS 'Person'