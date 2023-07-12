SELECT FROM Advertiser
WHERE out('CampaignList').out(WordSetList).out(WordList).wo_word CONTAINS 'secular'
AND out('CampaignList') CONTAINS (c_budget BETWEEN 10000 AND 15000)
AND out('CampaignList').out('ClickList').out('PersonClickList').p_id CONTAINS 17592186134210