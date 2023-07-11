cd "$( cd "$( dirname "${BASH_SOURCE[0]:-${(%):-%x}}" )" >/dev/null 2>&1 && pwd )"
cd ..

export NEO4J_CONTAINER_NAME=neo4j-community-5.9.0
export NEO4J_CONTAINER_ROOT={path to neo4j}
export NEO4J_CSV_POSTFIX=_0_0.csv
export NEO4J_DATA_DIR=${NEO4J_CONTAINER_ROOT}/data
export NEO4J_ENV_VARS={path to neo4j}
export NEO4J_VERSION=5.9.0

if [[ `uname -m` == "arm64" ]]; then
    export NEO4J_DOCKER_PLATFORM_FLAG="--platform linux/arm64"
else
    export NEO4J_DOCKER_PLATFORM_FLAG=""
fi

