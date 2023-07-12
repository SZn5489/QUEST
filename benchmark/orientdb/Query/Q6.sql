SELECT FROM Person WHERE p_id IN
(SELECT p_id FROM Person_r WHERE p_id IN
(SELECT p_id FROM (SELECT EXPAND(out('CampaignList').out('ClickList').out('PersonClickList')) FROM Advertiser))
AND p_credit_score BETWEEN 500 AND 550
AND p_wallet_banlance BETWEEN 10000 AND 15000)
AND out('StudyAt').out('IsLocatedIn').out('IsPartOf').co_name CONTAINS 'China'
AND out('LikesComment').out('HasTag').out('HasType').tc_name CONTAINS 'Person'