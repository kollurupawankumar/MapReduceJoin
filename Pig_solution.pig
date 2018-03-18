csv_file = LOAD 'cssd1.csv' USING PigStorage (',') AS (id, ua)
tsv_file = LOAD 'cssd2.tsv' USING PigStorage ('\t') AS (id, country, f1, f2, f3)
csv_tsv_join = JOIN csv_file by id, tsv_file by id

int_output = FOREACH csv_tsv_join GENERATE csv_file::ID, tsv_file::COUNTRY, csv_file::UA, tsv_file::F1, tsv_file::F2, tsv_file::F3

eu_file = LOAD 'EuCountries.txt' USING PigStorage (',') AS (code, country)
left_join_eu = JOIN int_output by country LEFT OUTER, eu_file by code
result = FILTER left_join_eu BY eu_file::code is null
STORE result INTO ‘myoutput.txt’ using PigStorage(',');