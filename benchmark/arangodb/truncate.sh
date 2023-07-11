arangosh --server.username "root" --server.password  ""  --server.endpoint "tcp://127.0.0.1:8532" --javascript.execute-string "db._dropDatabase('db_for_benchmark');"


arangosh --server.username "root" --server.password  ""  --server.endpoint "tcp://127.0.0.1:8532" --javascript.execute-string "db._createDatabase('db_for_benchmark'); 

db._useDatabase('db_for_benchmark'); db._create('Forum'); db._create('Message'); db._create('Organisation'); db._create('Person'); db._create('Place'); db._create('Tag'); db._create('TagClass'); db._createEdgeCollection('containerOf'); db._createEdgeCollection('hasCreator'); db._createEdgeCollection('hasInterest'); db._createEdgeCollection('hasMember'); db._createEdgeCollection('hasModerator'); db._createEdgeCollection('hasTag'); db._createEdgeCollection('hasType'); db._createEdgeCollection('isLocatedIn'); db._createEdgeCollection('isPartOf'); db._createEdgeCollection('isSubclassOf'); db._createEdgeCollection('knows'); db._createEdgeCollection('likes'); db._createEdgeCollection('replyOf'); db._createEdgeCollection('studyAt'); db._createEdgeCollection('workAt');"
