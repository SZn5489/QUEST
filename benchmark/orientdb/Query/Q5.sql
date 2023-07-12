SELECT FROM Person WHERE p_id IN
(SELECT p_id FROM Person_r WHERE p_id IN
(SELECT p_id FROM (SELECT EXPAND(out('CampaignList').out("ClickList").out("PersonClickList")) FROM Advertiser
WHERE out("CampaignList").out("WordSetList").out("WordList") CONTAINS (wo_word ='secular')
AND out('CampaignList') CONTAINS (c_budget BETWEEN 10000 AND 15000)))
AND p_credit_score BETWEEN 500 AND 550
AND p_wallet_banlance BETWEEN 10000 AND 15000)