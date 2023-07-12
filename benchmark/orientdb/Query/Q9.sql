SELECT FROM Person WHERE p_id IN
(SELECT p_id FROM Person_r WHERE p_id IN
(SELECT p_id FROM(SELECT EXPAND(out('CampaignList').out('ClickList').out('PersonClickList')) FROM Advertiser
WHERE out('CampaignList').out('ClickList') CONTAINS (cl_fee BETWEEN 5000 AND 8000)
AND out('CampaignList').out('ClickList') CONTAINS (cl_date BETWEEN '2018-01-01 00:00:00' AND '2020-01-01 00:00:00')))
AND p_credit_score BETWEEN 500 AND 550
AND p_wallet_banlance BETWEEN 10000 AND 15000)
AND out('StudyAt').or_name CONTAINS 'Central_University_of_Karnataka'
AND out('HasInterest').t_id CONTAINS 'Time3'