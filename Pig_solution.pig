-- loading the csv file
csv_file = LOAD 'cssd1.csv' USING PigStorage (',') AS (id, ua)

-- loading the tsv file
tsv_file = LOAD 'cssd2.tsv' USING PigStorage ('\t') AS (id, country, f1, f2, f3)

-- joining the file csv and tsv files
csv_tsv_join = JOIN csv_file by id, tsv_file by id

int_output = FOREACH csv_tsv_join GENERATE csv_file::id, tsv_file::country, csv_file::ua, tsv_file::f1, tsv_file::f2, tsv_file::f3

-- Loading the EU contries list and perform a let outer join
eu_file = LOAD 'EuCountries.txt' USING PigStorage (',') AS (code, country)
left_join_eu = JOIN int_output by country LEFT OUTER, eu_file by code

-- Filtering the result and storing it
result = FILTER left_join_eu BY eu_file::code is null
STORE result INTO ‘myoutput.txt’ using PigStorage(',');