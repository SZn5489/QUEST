Data Import:
  The Extractor Transformer and Loader, or ETL, module for OrientDB provides support for moving data to and from OrientDB databases using ETL processes.
  To use the ETL module, run the oetl.sh script with the configuration file given as an argument.
    $ORIENTDB_HOME/bin/oetl.sh config-dbpedia.json
  When you run the ETL module, you can define its configuration variables by passing it a JSON file, which the ETL module resolves at run-time by passing them as it starts up.
  The JSON files that are used to import the data used in the experiment are in the folder named 'ETL'.

Query Execution:
  There are two modes available to you, while executing commands through the OrientDB Console: interactive mode and batch mode.
  By default, the Console starts in interactive mode. In this mode, the Console loads to an orientdb> prompt. From there you can execute commands and SQL statements as you might expect in any other database console.
  You can launch the console in interactive mode by executing the console.sh for Linux OS systems or console.bat for Windows systems in the bin directory of your OrientDB installation. 
    $ cd $ORIENTDB_HOME/bin
    $ ./console.sh
    
    OrientDB console v.X.X.X (build 0) www.orientdb.com
    Type 'HELP' to display all the commands supported.
    Installing extensions for GREMLIN language v.X.X.X

    orientdb>
  From here, you can begin running SQL statements or commands. 
  The queries that are used in the experiment are in the folder named 'Query'.