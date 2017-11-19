REGISTER 'hdfs:/elephant-bird-hadoop-compat-4.1.jar';

REGISTER 'hdfs:/elephant-bird-pig-4.1.jar';

REGISTER 'hdfs:/json-simple-1.1.1.jar';

w = LOAD 'hdfs:/out.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS myMap;

ex = FOREACH w GENERATE FLATTEN(myMap#'entities') as (m:map[]),FLATTEN(myMap#'id') as id;

describe ex

hash = foreach ex generate FLATTEN(m#'hashtags') as(tags:map[]),id as id;

txt = foreach hash generate FLATTEN(tags#'text') as text,id;

grp = group txt by text;

cnt = foreach grp generate group as hashtag_text, COUNT(txt.text) as hashtag_cnt:int;
dump cnt;
STORE cnt INTO 'hdfs:/salidacontar';
