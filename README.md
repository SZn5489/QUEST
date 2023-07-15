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
### A. DETAILS OF MODIFIED TPCH QUERIES.
#### A.1 MQ6 and its variant parameters.
SELECT SUM(l.l_extendedprice * l.l_discount) as revenue

FROM LineItem AS l

WHERE l.l_shipdate BETWEEN @date1 AND @date2

AND l.l_discount BETWEEN @discount1 AND @discount2

AND l.l_quantity <= @quantity;

Table 11: Different selectivity by varying selection conditions in query MQ6.

Variables| @date1| @date2| @discount1| @discount2 |@quantity
--- | --- | --- | --- |--- |---
0.1 |"1992-01-01"| "1994-01-01"| 0 |0.04 |40
0.01 |"1993-01-01"| "1994-01-01"| 0.02| 0.04| 13
0.001| "1993-10-10"| "1994-01-01"| 0.03 |0.04 |9
0.0001| "1993-11-01" |"1994-01-01"| 0.04| 0.04| 3
0.00001| "1992-12-19"| "1994-01-01"| 0.04| 0.04| 2
#### A.2 MQ14 and its variant parameters.
SELECT p.p_type, l.l_extendedprice, l.l_discount

FROM Part AS p, LineItem AS l

WHERE p.p_partkey = l.l_partkey

AND l.l_shipdate >= @date1

AND l.l_shipdate <= @date2;

Table 12: Different selectivity by varying selection conditions in query MQ14.

Variables| @date1| @date2
--- | --- | --- 
0.1 |"1993-05-01"| "1994-01-01"
0.01 |"1993-11-01"| "1994-01-01"
0.001| "1991-11-25"| "1992-01-26"
0.0001| "1991-11-21" |"1992-01-09"
0.00001| "1992-11-25"| "1992-01-04"
#### A.3 MQ19 and its variant parameters.
SELECT l.l_extendedprice * l.l_discount

FROM Part AS p, LineItem AS l

WHERE p.p_partkey = l.l_partkey

AND p.p_brand <= @brand

AND p.p_container IN @cntset

AND p.p_size <= @size

AND l.l_shipinstruct = "DELIVER IN PERSON"

AND l.l_quantity BETWEEB @qty1 AND @qty2

AND l.l_shipmode IN @modeset;

Table 13: Different selectivity by varying selection conditions in query MQ19.

Variables| @brand| @cntset| @size| @qty1 |@qty2|@modeset
--- | --- | --- | --- |--- |--- |---
0.1 |"Brand#50"| {"SM","MED","LG","WRAP"}| 50 |0 |50|{"TRUCK","AIR","SHIP","FOB"}
0.01 |"Brand#40"| {"SM","MED","LG"}| 30 |10 |30|{"TRUCK","AIR","SHIP"}
0.001|"Brand#20"| {"SM","MED"}| 20 |10 |30|{"TRUCK","AIR"}
0.0001|"Brand#20"| {"SM"}| 20 |10 |20|{"TRUCK"}
0.00001|"Brand#14"| {"SM"}| 5 |10 |15|{"TRUCK"}
#### A.4 MQ20 and its variant parameters.
SELECT ps.ps_suppkey, ps.ps_availqty

FROM Part AS p, PartSupp AS ps, LineItem AS l

WHERE p.p_partkey = ps.ps_partkey

AND ps.ps_partkey = l.l_partkey

AND ps.ps_suppkey = l.l_suppkey

AND p.p_name IN @nameset

AND l.l_shipdate >= @shipdate1

AND l.l_shipdate <= @shipdate2;

Table 14: Different selectivity by varying selection conditions in query MQ20.

Variables| @nameset| @shipdate1| @shipdate2
--- | --- | --- | --- 
0.1 | {"green","lemon","red"}| "1992-01-01" |"1998-01-01"
0.01 | {"green"}| "1992-09-10" |"1993-01-01"
0.001|  {"sandy"}| "1992-10-20" |"1992-12-10"
0.0001 | {"sandy"}| "1992-12-05" |"1992-12-10"
0.00001 | {"sandy"}| "1992-10-24" |"1998-12-10"
### B. Modified PTCH QUERIES ON NESTED SCHEMAS.
#### B.1 P-PS-L: nested schema for Part, Partsupp, Lineitem with seudo mapping scheme.
{"type": "record", "name": "PPSL", 

 "fields": [ 
 
     {"name": "p_partkey", "type": "int", "order": "ignore"}, 
     
     {"name": "p_name", "type": "string"}, 
     
     {"name": "p_mfgr", "type": "string"}, 
     
     {"name": "p_brand", "type": "string"}, 
     
     {"name": "p_type", "type": "string"}, 
     
     {"name": "p_size", "type": "int"}, 
     
     {"name": "p_container", "type": "string"}, 
     
     {"name": "p_retailprice", "type": "float"}, 
     
     {"name": "p_comment", "type": "string"}, 
     
     {"name": "pslst", "type":{"type": "array", 
     
      "items":{"type": "record", "name": "ps", 
      
      "fields": [ 
      
          {"name": "ps_partkey", "type": ["int", "null"], "order": "ignore"}, 
          
          {"name": "ps_suppkey", "type": ["int", "null"]}, 
          
          {"name": "ps_availqty", "type": ["int", "null"]}, 
          
          {"name": "ps_supplycost", "type": ["float", "null"]}, 
          
          {"name": "ps_comment", "type": ["string", "null"]}, 
          
          {"name": "llst", "type":{"type": "array", 
          
           "items": {"type": "record", "name": "l", 
           
           "fields": [ 
           
               {"name": "l_orderkey", "type": "int", "order": "ignore"}, 
               
               {"name": "l_partkey", "type": "int"}, 
               
               {"name": "l_suppkey", "type": "int"}, 
               
               {"name": "l_linenumber", "type": "int"}, 
               
               {"name": "l_quantity", "type": "float"}, 
               
               {"name": "l_extendedprice", "type": "float"}, 
               
               {"name": "l_discount", "type": "float"}, 
               
               {"name": "l_tax", "type": "float"}, 
               
               {"name": "l_returnflag", "type": "bytes"}, 
               
               {"name": "l_linestatus", "type": "bytes"}, 
               
               {"name": "l_shipdate", "type": "string"}, 
               
               {"name": "l_commitdate", "type": "string"}, 
               
               {"name": "l_receiptdate", "type": "string"}, 
               
               {"name": "l_shipinstruct", "type": "string"}, 
               
               {"name": "l_shipmode", "type": "string"}, 
               
               {"name": "l_comment", "type": "string"} 
               
          ]}}} 
          
     ]}}} 
     
]}

#### B.2 MQ06 on PPSL schema.
SELECT SUM(d.pslst.ps.llst.l.l_extendedprice * d.pslst.ps.llst.l.l_discount)

FROM PPSL AS d

WHERE d.pslst.any:ps.llst.any:l.l_shipdate BETWEEN @date1 AND @date2

AND d.pslst.any:ps.llst.any:l.l_discount BETWEEN @dc1 AND @dc2

AND d.pslst.any:ps.llst.any:l.l_quantity <= @quantity;
#### B.3 MQ14 on PPSL schema.
SELECT d.p_type, d.pslst.ps.llst.l.l_extendedprice, d.pslst.ps.llst.l.l_discount

FROM PPSL AS d

WHERE d.pslst.any:ps.llst.any:l.l_shipdate BETWEEN @date1 AND @date2;
#### B.4 MQ19 on PPSL schema.
SELECT d.pslst.ps.llst.l.l_extendedprice * d.pslst.ps.llst.l.l_discount

FROM PPSL AS d

WHERE d.p_brand <= @brand AND d.p_container IN @cntset

AND d.p_size <= @size

AND d.pslst.any:ps.llst.any:l.l_shipinstruct = "DELIVER IN PERSON"

AND d.pslst.any:ps.llst.any:l.l_quantity BWTWEEN @qty1 AND @qty2

AND d.pslst.any:ps.llst.any:l.l_shipmode IN @modeset;
#### B.5 MQ20 on PPSL schema.
SELECT d.pslst.ps.ps_suppkey, d.pslst.ps.llst.l.ps_availqty

FROM PPSL AS d

WHERE d.p_name IN @nameset

AND d.pslst.any:ps.llst.any:l.l_shipdate BETWEEM @date1 AND @date2;
