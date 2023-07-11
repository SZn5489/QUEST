
arangosh --server.username "root" --server.password  ""  --server.endpoint "tcp://127.0.0.1:8532" --javascript.execute-string "db._useDatabase('db_for_benchmark');db._drop('Relational_Person');";
arangosh --server.username "root" --server.password  ""  --server.endpoint "tcp://127.0.0.1:8532" --javascript.execute-string "db._useDatabase('db_for_benchmark');db._create('Relational_Person');";

data_file={path to data dir}/data/relation/person.csv
headers_file={path to headers}/headers
#_key|first_name|last_name|credit_score|wallet_balances
arangoimport --server.username "root" --server.password  ""  --server.endpoint "tcp://127.0.0.1:8532" --server.database db_for_benchmark \
--file ${data_file} --headers-file ${headers_file} --type auto  --separator "|" --collection Relational_Person ; 

