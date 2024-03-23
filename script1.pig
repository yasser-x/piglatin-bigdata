--charger les données du fichier csv
data=LOAD '/home/cloudera/pig_lab/input/vol.csv Using PigStorage(';') AS (annee:int , mois:int, jour:int, num_vol:chararray, aeroport_depart:chararray, aeroport_arrivee:chararray, distance:int);
--Regrouper les vols par aéroport de départ et compter le nombre de vols grouped_data=GROUP data by aeroport_depart;
flight_count=FOREACH grouped_data GENERATE group AS nom_aeroport , COUNT(data) AS nb_vol;
--Afficher le résultat
DUMP flight_count;
--FIltrer les vols par aéroport de départ donné en paramètre
filtred_data=FILTER data BY aeroport_depart=='$aeroport_parametre';
--Compter le nombre de vols 
flight_count=FOREACH(GROUP filtered_data ALL) GENERATE COUNT(filtered_data)  AS nb_vol ;
--Afficher le résultat
DUMP flight_count ;
--Trouver la distance maximale
max_distance=FOREACH (ORDER data BY distance DESC) GENERATE distance AS distance_max 
