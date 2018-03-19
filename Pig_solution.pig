-- loading the csv file
csv_file = LOAD '/ccds1.csv' USING TextLoader() AS (line:chararray);
csv_file_main = FOREACH csv_file GENERATE FLATTEN(STRSPLIT(line, '","'));
csv_file_res = FOREACH csv_file_main GENERATE REPLACE($0,'"',''), REPLACE($1,'"','');



-- loading the tsv file
tsv_file = LOAD '/ccds2.tsv' USING PigStorage ('\t') AS (id, country, f1, f2, f3);

-- joining the file csv and tsv files
csv_tsv_join = JOIN csv_file_res by $0, tsv_file by id;

int_output = FOREACH csv_tsv_join GENERATE tsv_file::id, tsv_file::country, $1, tsv_file::f1, tsv_file::f2, tsv_file::f3;

-- Loading the EU contries list and perform a let outer join
eu_file = LOAD '/EuCountries.txt' USING PigStorage (',') AS (code, country);
left_join_eu = JOIN int_output by country LEFT OUTER, eu_file by code;

-- Filtering the result and storing it
result = FILTER left_join_eu BY eu_file::code is null;
STORE result INTO ‘myoutput.txt’ using PigStorage(',');