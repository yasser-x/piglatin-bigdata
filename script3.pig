--charger les données du fichier maman.txt
data =LOAD 'home/cloudera/pig_lab/input/maman.txt' USING TextLoader() AS (line:chararray);

--Filtrer les lignes contenant le mot recherché
filtered_data =FILTER data BY REGEX_EXTRAT(line, '(.*)your_search_word(.*)',0) IS NOT NULL 

--Compter le nombre d'occurrences du mot 
word_count = FOREACH (GROUP filtered_data ALL) GENERATE COUNT(filtered_data) AS occurence_count;

--Afficher le résultat 
DUMP word_count;
