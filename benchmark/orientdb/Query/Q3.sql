SELECT FROM Person Where p_id IN
(SELECT p_id FROM Person_r WHERE p_id IN
(SELECT p_id FROM (SELECT EXPAND(out('CampaignList').out("ClickList").out("PersonClickList")) FROM Advertiser
WHERE out("CampaignList").out("WordSetList").out("WordList") CONTAINS (wo_word ='secular')
AND out('CampaignList') CONTAINS (c_budget BETWEEN 10000 AND 15000)
AND out('CampaignList').out('ClickList') CONTAINS (cl_date BETWEEN '2018-01-01 00:00:00' AND '2020-01-01 00:00:00')))
AND p_credit_score BETWEEN 500 AND 550)
AND out('LikesComment').out('HasTag').out('HasType').tc_name CONTAINS 'Person' 