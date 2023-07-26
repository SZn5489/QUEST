QUEST
=====
Introduction
-----
    This project, QUEST (Query Evaluation Scheme Towards scan-intensive cross-model analysis), aims at pushing scan-intensive queries down to unified columnar storage layout and seamlessly deliver payloads across different data models. The key idea behind QUEST is to leverage columnar storage layout and advanced column-oriented techniques to develop customi zed evaluation scheme for scan-intensive cross-model queries. 
    
The proposed methods are implemented on an open-source platform. Through comprehensive theoretical analysis and extensive experiments, we demonstrate that QUEST improves the performance by 3.7x-178.2x compared to state-of-the-art multi-model databases when evaluating scan-intensive cross-model analytical queries.

Important Implementations
-----

1. We first unify the logical data model of the relational, nested document-based data and property graph-based data based on the extended recursive definition of nested tree-structured data, and develop a lossless representation of record structure in a columnar format. _Counter_ and _Indicator_ arrays stored in columns are utilized to maintain the mapping information between adjacent layers in nested model. A novel index structure _Skip-Tree_ is developed to preserve the pre-computed mapping information across nested layers.

2. We present a novel column-oriented skipping scheme based on _Skip-Tree_ structure and bitset-based query storage pushdown strategy, generalized by a two pair-wise operations, _SkipUp_ and _SkipDown_. It can significantly reduce both I/O and CPU overheads when processing scan-intensive analytical workloads by pruning the scan of irrelevant instances. We also introduce a way to seamlessly deliver query payloads across different models.

3. We delve into the query evaluation costs of QUEST and establish a comprehensive cost model that encompasses Modern data-driven applications require that databases support fast cross-model analytical queries. 

State-of-the-art Comparison
-----
### Codebase of the comparisons.
Based on newly generated multi-model data, we simulate a holistic analysis on users’ social behaviors and advertisers’ promotional campaigns. And referring to the choke points design of popular benchmarks such as LDBC and TPC-H, we further refine the choke points of scan-intensive cross-model analysis.

![Figure 1: Choke points design](https://github.com/SZn5489/QUEST/assets/119804426/c2fd42c4-ca87-41e2-b96d-2bd1d68efe3e)

Figure 1: Choke points design

In order to further test the efficiency of our proposed evaluation scheme, comprehensively test the pros and cons of QUEST, we design customized query loads, focusing on the impact of selectivity and nested depth of predicates on the query evaluating efficiency. Overall, the workload description is shown in Table 1.

Table 1: Workload description (R/D/G represents the number of predicates in Relation/Document/Graph model)

Queries| R/D/G| Selectivity| Nested depth| Choke Points
--- | --- | --- | --- |---
Q1| 2/2/2| high| deep| __1__, __2__, __3__, __4__, __5__, __6__, __7__
Q2| 3/1/1| high| deep| 1, 2, 3, __4__, 5, 6, 7
Q3| 1/3/1| high| deep| 1, 2, 3, __4__, 5, 6, 7
Q4| 1/1/3| high| deep| 1, 2, 3, __4__, 5, 6, 7
Q5| 2/2/0| high| deep| __2__, 3, __4__, 5, 6, 7
Q6| 2/0/2| high| deep| __2__, 3, __4__, 5, 6, 7
Q7| 0/2/2| high| deep| __2__, 3, __4__, 5, 6, 7
Q8| 2/2/2| low| deep| 1, 2, 3, 4, 5, __6__, 7
Q9| 2/2/2| high| shallow| 1, 2, 3, 4, 5, 6, __7__
Q10| 0/3/0| high| deep| __3__, __5__, 6, 7
Q11| 0/0/3| high| deep| __3__, __5__, 6, 7

The mainstream MMDBs are selected as the main comparison objects, including Arango-DB-community-3.10.8 and OrientDB community-3.2.20. In addition, we map multi-model data to relational paradigm and graph pattern by trivial data modeling, and conduct more detailed experiments on a column-oriented relational database ClickHouse community-23.5.3.24 together with a graph database Neo4j community-5.9.0 which show excellent analytical performance on individual data type, to further explore the pros and cons of various queries evaluation schemes in dealing with scan-intensive cross-model analysis. In all experiment, we use default indexes which are built on primary keys, and no secondary index is created. The experimental codes are as follows.

[project](https://github.com/SZn5489/QUEST "https://github.com/SZn5489/QUEST")

[QUEST test](https://github.com/SZn5489/QUEST/tree/master/benchmark/QUEST "https://github.com/SZn5489/QUEST/tree/master/benchmark/QUEST")

[ArangoDB test](https://github.com/SZn5489/QUEST/tree/master/benchmark/arangodb "https://github.com/SZn5489/QUEST/tree/master/benchmark/arangodb")

[OrientDB test](https://github.com/SZn5489/QUEST/tree/master/benchmark/orientdb "https://github.com/SZn5489/QUEST/tree/master/benchmark/orientdb")

[ClickHouse test](https://github.com/SZn5489/QUEST/tree/master/benchmark/clickhouse "https://github.com/SZn5489/QUEST/tree/master/benchmark/clickhouse")

[Neo4j test](https://github.com/SZn5489/QUEST/tree/master/benchmark/neo4j "https://github.com/SZn5489/QUEST/tree/master/benchmark/neo4j")

### Performance evaluation.
#### Query running time.
In all scan-intensive cross-model query evaluation experiments, QUEST out performs all MMDBs in query running time and improves the performance by 3.7 × −178.2×. It is only slightly slower than the two analytical databases Neo4j and Clickhouse in the query of 6 and 9. QUEST has the most stable performance among all competitors as it keeps the running time under 25 seconds in all queries, while the rest databases may incur severe query latency timeouts when evaluating specific scan-intensive cross-model queries (we set 2000 second as the upper time limit). The excellent analytical performance mainly gains from the column-orient skipping scheme which enables QUEST to push scan-intensive cross-model queries down to storage and prune the scan of most irrelevant instance.

![Figure 2: Query running time](https://github.com/SZn5489/QUEST/assets/119804426/d264c258-83e4-4e4a-8e6a-25a837656ebb)

Figure 2: Query running time

#### Memory usage. 
Although the memory usage of QUEST is not the least in most of scan-intensive cross-model analytical queries, it is kept in a pretty low section between 1.66 GB to 16.91 GB, second only to OrientDB. QUEST is not like other competitors to generate huge intermediate results and incur memory crash in specific scan intensive cross-model queries. This mainly credits to bitset-based payload delivery strategy, which significantly reduces the scale intermediate results caused by cross-model joins. The results verify QUEST’s robustness in memory usage when evaluating the scan intensive cross-model analysis. 

![Figure 3: Memory Usage](https://github.com/SZn5489/QUEST/assets/119804426/daee2ffb-d809-4c83-a3c8-b651e61399f5)

Figure 3: Memory Usage

#### Disk space overhead. 
QUEST takes 17.83 GB disk space to store 20.24 GB multi-model data which includes extra 2.85 GB space of Skip-tree index. However, the disk space overhead is still significantly lower than other MMDBs, second only to ClickHouse. Although QUEST does not focus on apply advanced column-orient compression scheme to multi-model data, it naturally comes as an additional benefit of utilizing a unified column-based storage. We leave the efficient direct processing on compressed data as future work. 

Appendex: Workloads and their parameters
-----
### Q1
```sql
select Person 
From Ralation as R, Document as D, Graph as G
where R.Credit_Score in 500-550 and R.Wallet_Balances in 10000-15000
  and D.word = 'secular' and D.budget in 10000-15000
  and G.person.Studyat...Country = 'China' and G.person.likes...tag.tagclass = 'Person'
```
### Q2
```sql
select Person 
From Ralation as R, Document as D, Graph as G
where R.Credit_Score in 500-550 and R.Wallet_Balances in 10000-15000 and R.fname = 'Lin'
  and D.word = 'secular' 
  and G.person.likes...tag.tagclass ='Person'
```
### Q3
```sql
select Person 
From Ralation as R, Document as D, Graph as G
where R.Credit_Score in 500-550
  and D.word = 'secular' and D.budget in 10000-15000 and D.Date in 2018-01-01 00:00:00-2020-01-01 00:00:00
  and G.person.likes...tag.tagclass ='Person'
```
### Q4
```sql
select Person 
From Ralation as R, Document as D, Graph as G
where R.Credit_Score in 500-550
  and D.word = 'secular'
  and G.person.likes...tag.tagclass = 'Person' and G.person.Studyat...Country = 'China' and G.person.knows.person.hasInterest.tag.tagclass = 'Person'
```
### Q5
```sql
select Person 
From Ralation as R, Document as D, Graph as G
where R.Credit_Score in 500-550 and R.Wallet_Balances in 10000-15000
      D.word = 'secular' and D.budget in 10000-15000 
```
### Q6
```sql
select Person 
From Ralation as R, Document as D, Graph as G
where R.Credit_Score in 500-550 and R.Wallet_Balances in 10000-15000
  and G.person.Studyat...Country = 'China' and G.person.likes...tag.tagclass = 'Person' 
```
### Q7
```sql
select Person 
From Ralation as R, Document as D, Graph as G
where D.word = 'secular' and D.budget in 10000-15000
  and G.person.Studyat...Country = 'China' and G.person.likes...tag.tagclass 'Person'
```
### Q8
```sql
select Person 
From Ralation as R, Document as D, Graph as G
where R.Credit_Score in 300-800 and R.Wallet_Balances in 5000-50000
  and D.word = 'secular' and D.budget in 5000-25000
  and G.person.Studyat...Country = 'China' and G.person.likes...tag.tagclass = 'Person'
```
### Q9
```sql
select Person 
From Ralation as R, Document as D, Graph as G
where R.Credit_Score in 500-550 and R.Wallet_Balances in 10000-15000
  and D.Fee in 5000-8000 and D.Date in 2018-01-01 00:00:00-2020-01-01 00:00:00
  and G.person.Studyat.university.name = 'Central_University_of_Karnataka' and G.person.hasInterest.tag = 'Time3'
```
### Q10
```sql
select Advertiser 
From Document as D
where D.word = 'secular' and D.budget in 10000-15000 and D.PID = 17592186134210
```
### Q11
```sql
select Person 
From Graph as G
where  G.person.Studyat...Country = 'China' and G.person.likes...tag.tagclass = 'Person' and G.person.knows.person.hasInterest.tag.tagclass = 'Person'
```
