doc_file={path to data}/all.jsonl
arangosh --server.username "root" --server.password  ""  --server.endpoint "tcp://127.0.0.1:8532" --javascript.execute-string \
"db._useDatabase('db_for_benchmark');db._create('Doc_Adv');";
arangoimport --server.username "root" --server.password  ""  --server.endpoint "tcp://127.0.0.1:8532" --server.database db_for_benchmark \
--file ${doc_file} --type jsonl  --collection 'Doc_Adv' --progress true --threads 36  --on-duplicate ignore; 
