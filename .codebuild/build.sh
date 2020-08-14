#!/usr/bin/env sh
set -ex

echo Running Snow Changes...
docker run -t --rm \
  --name snowchange-script \
  -v "$PWD":/usr/src/snowchange \
  -w /usr/src/snowchange \
  -e ROOT_FOLDER \
  -e SNOWFLAKE_ACCOUNT \
  -e SNOWFLAKE_USER \
  -e SNOWFLAKE_ROLE \
  -e SNOWFLAKE_WAREHOUSE \
  -e SNOWFLAKE_REGION \
  -e SNOWSQL_PWD \
  python:3 /bin/bash -c "pip install --upgrade snowflake-connector-python && python snowchange.py -f $ROOT_FOLDER -a $SNOWFLAKE_ACCOUNT --snowflake-region $SNOWFLAKE_REGION -u $SNOWFLAKE_USER -r $SNOWFLAKE_ROLE -w $SNOWFLAKE_WAREHOUSE"