SELECT FROM Person
WHERE out('StudyAt').out('IsLocatedIn').out('IsPartOf').co_name CONTAINS 'China'
AND out('LikesComment').out('HasTag').out('HasType').tc_name CONTAINS 'Person'
AND out('Knows').out('HasInterest').out('HasType').tc_name CONTAINS 'Person'