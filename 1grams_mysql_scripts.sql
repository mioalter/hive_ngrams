CREATE DATABASE 1grams;

CREATE TABLE grams_lb(
gram char(255) not null,
year int not null,
ratio float not null,
increase float not null
) Engine=MyISAM;

LOAD DATA LOCAL INFILE '/Users/mioalter/Documents/Hadoop_data/1grams_v2' INTO TABLE grams_lb;

SELECT gram,year,ratio,increase 
FROM grams_lb
WHERE year=1985
LIMIT 100;

SELECT gram,year,ratio,increase
FROM grams_lb
WHERE gram='microprocessor';


CREATE TABLE grams_decades(
gram char(255) not null,
decade int not null,
ratio float not null,
increase float not null
) Engine=MyISAM;

LOAD DATA LOCAL INFILE '/Users/mioalter/Documents/Hadoop_data/1grams_decades' INTO TABLE grams_decades;

SELECT gram,decade,ratio,increase 
FROM grams_decades
WHERE decade=198
LIMIT 100;


CREATE TABLE grams_new(
gram char(255) not null,
decade int not null,
ratio float not null
) Engine=MyISAM;

LOAD DATA LOCAL INFILE '/Users/mioalter/Documents/Hadoop_data/decades_new_words' INTO TABLE grams_new;

SELECT gram,decade 
FROM grams_new
WHERE decade=196
LIMIT 100;
