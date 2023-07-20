## benchmark of QUEST
### 1. data processing
After placing the source data in the *origin_data* folder, execute the following command
~~~bash
cd ./bash/datagen
bash DocGen.sh
bash GraphGen.sh
bash RelationGen.sh
bash GenMeta.sh
~~~
The default memory parameter of these commands is set to 40GB. The processed data is stored in the *QUEST/data/* directory.
### 2. query
After data processing, enter the folder named *query* and run the bash script corresponding to the query. 
*Q1.sh*-*Q11.sh* are queries made on the QUEST system. *CORESQ10.sh* and *NoQ11.sh* are the Q10 query and Q11 query made on the system without the skip-tree index respectively.  
*ParquetQ10.sh* is the Q10 query made on the format named "Parquet".
