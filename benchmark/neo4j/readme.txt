Part of the scripts are from the project https://github.com/PlatformLab/ldbc-snb-impls
Before executing the scripts, add the following contents to ~/.bashrc file and execute source ~/.bashrc 
export NEO4J_CONVERTED_CSV_DIR="{path to data dir}/data/ldbc/converted_for_neo4j"
export NEO4J_VANILLA_CSV_DIR="{path to data dir}/data/ldbc/csv"
export NEO4J_CONTAINER_ROOT="{path to neo4j}/neo4j/neo4j-community-5.9.0"
export NEO4J_CSV_POSTFIX="_0_0.csv"

To import ldbc dataset, run the load-in-one-step.sh in ldbc_import/scripts
To import the rest data, run convert_my_csvs.sh, and run import_graph_and_doc.sh