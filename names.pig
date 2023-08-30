names = LOAD 's3://emr-pig-bucket-1/names.txt' AS (name:chararray);
namesWithLength = FOREACH names GENERATE name, SIZE(name);
STORE namesWithLength INTO 's3://emr-pig-bucket-1/output/pig/';
