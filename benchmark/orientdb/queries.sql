/* Q1 */
SELECT FROM Person WHERE p_id IN
(SELECT p_id FROM Person_r WHERE p_id IN 
(SELECT p_id FROM (SELECT EXPAND(out('CampaignList').out("ClickList").out("PersonClickList")) FROM Advertiser 
WHERE out('CampaignList') CONTAINS (c_budget BETWEEN 10000 AND 15000) 
AND out("CampaignList").out("WordSetList").out("WordList") CONTAINS (wo_word ='secular')))
AND p_credit_score BETWEEN 500 AND 550
AND p_wallet_banlance BETWEEN 10000 AND 15000)
AND out('StudyAt').out('IsLocatedIn').out('IsPartOf').co_name CONTAINS 'China'
AND out('LikesComment').out('HasTag').out('HasType').tc_name CONTAINS 'Person'

/* Q2 */
SELECT FROM Person WHERE p_id IN
(SELECT p_id FROM Person_r WHERE p_id IN
(SELECT p_id FROM (SELECT EXPAND(out('CampaignList').out("ClickList").out("PersonClickList")) FROM Advertiser 
WHERE out("CampaignList").out("WordSetList").out("WordList") CONTAINS (wo_word ='secular')))
AND p_credit_score BETWEEN 500 AND 550
AND p_wallet_banlance BETWEEN 10000 AND 15000
AND p_fname = 'Lin')
AND out('LikesComment').out('HasTag').out('HasType').tc_name CONTAINS 'Person'

/* Q3 */
SELECT FROM Person Where p_id IN
(SELECT p_id FROM Person_r WHERE p_id IN
(SELECT p_id FROM (SELECT EXPAND(out('CampaignList').out("ClickList").out("PersonClickList")) FROM Advertiser
WHERE out("CampaignList").out("WordSetList").out("WordList") CONTAINS (wo_word ='secular')
AND out('CampaignList') CONTAINS (c_budget BETWEEN 10000 AND 15000)
AND out('CampaignList').out('ClickList') CONTAINS (cl_date BETWEEN '2018-01-01 00:00:00' AND '2020-01-01 00:00:00')))
AND p_credit_score BETWEEN 500 AND 550)
AND out('LikesComment').out('HasTag').out('HasType').tc_name CONTAINS 'Person' 

/* Q4 */
SELECT FROM Person WHERE p_id IN
(SELECT p_id FROM Person_r WHERE p_id IN
(SELECT p_id FROM (SELECT EXPAND(out('CampaignList').out("ClickList").out("PersonClickList")) FROM Advertiser
WHERE out("CampaignList").out("WordSetList").out("WordList") CONTAINS (wo_word ='secular')))
AND p_credit_score BETWEEN 500 AND 550)
AND out('LikesComment').out('HasTag').out('HasType').tc_name CONTAINS 'Person'
AND out('StudyAt').out('IsLocatedIn').out('IsPartOf').co_name CONTAINS 'China'
AND out('Knows').out('HasInterest').out('HasType').tc_name CONTAINS 'Person'

/* Q5 */
SELECT FROM Person WHERE p_id IN
(SELECT p_id FROM Person_r WHERE p_id IN
(SELECT p_id FROM (SELECT EXPAND(out('CampaignList').out("ClickList").out("PersonClickList")) FROM Advertiser
WHERE out("CampaignList").out("WordSetList").out("WordList") CONTAINS (wo_word ='secular')
AND out('CampaignList') CONTAINS (c_budget BETWEEN 10000 AND 15000)))
AND p_credit_score BETWEEN 500 AND 550
AND p_wallet_banlance BETWEEN 10000 AND 15000)

/* Q6 */
SELECT FROM Person WHERE p_id IN
(SELECT p_id FROM Person_r WHERE p_id IN
(SELECT p_id FROM (SELECT EXPAND(out('CampaignList').out('ClickList').out('PersonClickList')) FROM Advertiser))
AND p_credit_score BETWEEN 500 AND 550
AND p_wallet_banlance BETWEEN 10000 AND 15000)
AND out('StudyAt').out('IsLocatedIn').out('IsPartOf').co_name CONTAINS 'China'
AND out('LikesComment').out('HasTag').out('HasType').tc_name CONTAINS 'Person'

/* Q7 */
SELECT FROM Person WHERE p_id IN
(SELECT p_id FROM (SELECT EXPAND(out('CampaignList').out('ClickList').out('PersonClickList')) FROM Advertiser
WHERE out('CampaignList').out(WordSetList).out(WordList).wo_word CONTAINS 'secular'
AND out('CampaignList') CONTAINS (c_budget BETWEEN 10000 AND 15000)))
AND out('StudyAt').out('IsLocatedIn').out('IsPartOf').co_name CONTAINS 'China'
AND out('LikesComment').out('HasTag').out('HasType').tc_name CONTAINS 'Person'

/* Q8 */
SELECT FROM Person WHERE p_id IN
(SELECT p_id FROM Person_r WHERE p_id IN
(SELECT p_id FROM (SELECT EXPAND(out('CampaignList').out("ClickList").out("PersonClickList")) FROM Advertiser
WHERE out("CampaignList").out("WordSetList").out("WordList") CONTAINS (wo_word ='secular')
AND out('CampaignList') CONTAINS (c_budget BETWEEN 5000 AND 25000)))
AND p_credit_score BETWEEN 300 AND 800
AND p_wallet_banlance BETWEEN 5000 AND 50000)
AND out('StudyAt').out('IsLocatedIn').out('IsPartOf').co_name CONTAINS 'China'
AND out('LikesComment').out('HasTag').out('HasType').tc_name CONTAINS 'Person'

/* Q9 */
SELECT FROM Person WHERE p_id IN
(SELECT p_id FROM Person_r WHERE p_id IN
(SELECT p_id FROM(SELECT EXPAND(out('CampaignList').out('ClickList').out('PersonClickList')) FROM Advertiser
WHERE out('CampaignList').out('ClickList') CONTAINS (cl_fee BETWEEN 5000 AND 8000)
AND out('CampaignList').out('ClickList') CONTAINS (cl_date BETWEEN '2018-01-01 00:00:00' AND '2020-01-01 00:00:00')))
AND p_credit_score BETWEEN 500 AND 550
AND p_wallet_banlance BETWEEN 10000 AND 15000)
AND out('StudyAt').or_name CONTAINS 'Central_University_of_Karnataka'
AND out('HasInterest').t_id CONTAINS 'Time3'

/* Q10 */
SELECT FROM Advertiser
WHERE out('CampaignList').out(WordSetList).out(WordList).wo_word CONTAINS 'secular'
AND out('CampaignList') CONTAINS (c_budget BETWEEN 10000 AND 15000)
AND out('CampaignList').out('ClickList').out('PersonClickList').p_id CONTAINS 17592186134210

/* Q11 */
SELECT FROM Person
WHERE out('StudyAt').out('IsLocatedIn').out('IsPartOf').co_name CONTAINS 'China'
AND out('LikesComment').out('HasTag').out('HasType').tc_name CONTAINS 'Person'
AND out('Knows').out('HasInterest').out('HasType').tc_name CONTAINS 'Person'