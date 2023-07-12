SELECT FROM Person WHERE p_id IN
(SELECT p_id FROM (SELECT EXPAND(out('CampaignList').out('ClickList').out('PersonClickList')) FROM Advertiser
WHERE out('CampaignList').out(WordSetList).out(WordList).wo_word CONTAINS 'secular'
AND out('CampaignList') CONTAINS (c_budget BETWEEN 10000 AND 15000)))
AND out('StudyAt').out('IsLocatedIn').out('IsPartOf').co_name CONTAINS 'China'
AND out('LikesComment').out('HasTag').out('HasType').tc_name CONTAINS 'Person'