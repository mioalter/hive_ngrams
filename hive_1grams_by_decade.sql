set hive.base.inputformat=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set mapred.min.split.size=134217728;
set outputdecades=s3n://miobucket/tables/output_decades;
set outputnew=s3n://miobucket/tables/output_new_words;




CREATE EXTERNAL TABLE ngrams (
 gram string,
 year int,
 occurrences bigint,
 pages bigint,
 books bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS SEQUENCEFILE
LOCATION 's3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/1gram/';




CREATE TABLE normalized (
 gram string,
 year int,
 occurrences bigint
);




INSERT OVERWRITE TABLE normalized
SELECT
 lower(gram),
 year,
 occurrences
FROM
 ngrams
WHERE
 year >= 1890 AND
 gram REGEXP "^[A-Za-z+'-]+$";



CREATE TABLE by_decade (
 gram string,
 decade int,
 ratio double
);



INSERT OVERWRITE TABLE by_decade
SELECT
 a.gram,
 b.decade,
 sum(a.occurrences) / b.total
FROM
 normalized a
JOIN ( 
 SELECT 
  substr(year, 0, 3) as decade, 
  sum(occurrences) as total
 FROM 
  normalized
 GROUP BY 
  substr(year, 0, 3)
) b
ON
 substr(a.year, 0, 3) = b.decade
GROUP BY
 a.gram,
 b.decade,
 b.total;


CREATE EXTERNAL TABLE IF NOT EXISTS output_decades (
gram string,
decade int,
ratio double,
increase double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hiveconf:outputdecades}';




INSERT OVERWRITE TABLE output_decades
SELECT
 a.gram as gram,
 a.decade as decade,
 a.ratio as ratio,
 a.ratio / b.ratio as increase
FROM 
 by_decade a 
JOIN 
 by_decade b
ON
 a.gram = b.gram and
 a.decade - 1 = b.decade
WHERE
 a.ratio > 0.000001 and
 a.decade >= 190
DISTRIBUTE BY
 decade
SORT BY
 decade ASC,
 increase DESC;





CREATE EXTERNAL TABLE IF NOT EXISTS output_new_words (
gram string,
decade int,
ratio double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hiveconf:outputnew}';


INSERT OVERWRITE TABLE output_new_words
SELECT
 a.gram as gram,
 a.decade as decade,
 a.ratio as ratio
FROM 
 by_decade a 
LEFT OUTER JOIN 
 by_decade b
ON
 a.gram = b.gram and
 a.decade - 1 = b.decade
WHERE
 a.decade >= 190 AND
 b.ratio IS NULL
DISTRIBUTE BY
 decade
SORT BY
 decade ASC,
 ratio DESC;

