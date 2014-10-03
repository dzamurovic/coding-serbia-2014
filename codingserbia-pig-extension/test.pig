REGISTER target/codingserbia-pig-extension-1.0.0-SNAPSHOT.jar

--DEFINE JsonLoader com.codingserbia.JsonLoader();

test = LOAD '/some/path/customer_records_S.json' USING com.codingserbia.JsonLoader();
DUMP test;
