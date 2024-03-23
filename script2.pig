-- Charger les données du fichier bank.csv
data = LOAD 'bank.csv' USING PigStorage(';') AS (banque:chararray, client:chararray, versement:int, retrait:int, annee:int);

-- Filtrer les clients débiteurs (ceux avec des retraits)
debitors = DISTINCT FILTER data BY retrait > 0;

-- Extraire et afficher les identifiants des clients débiteurs
client_ids = FOREACH debitors GENERATE client;
DUMP client_ids;

-- Charger les données du fichier bank.csv
data = LOAD 'bank.csv' USING PigStorage(';') AS (banque:chararray, client:chararray, versement:int, retrait:int, annee:int);

-- Regrouper les clients par banque et compter le nombre de clients
clients_per_bank = DISTINCT FOREACH (GROUP data BY banque) GENERATE group AS banque, COUNT(data.client) AS nombre_clients;
DUMP clients_per_bank;

-- Charger les données du fichier bank.csv
data = LOAD 'bank.csv' USING PigStorage(';') AS (banque:chararray, client:chararray, versement:int, retrait:int, annee:int);

-- Filtrer les clients débiteurs (ceux avec des retraits) et les distincts
debitors = DISTINCT FILTER data BY retrait > 0;

-- Regrouper les clients débiteurs par banque et compter le nombre de clients distincts
debitors_per_bank = DISTINCT FOREACH (GROUP debitors BY banque) GENERATE group AS banque, COUNT(debitors.client) AS nombre_clients_debiteurs;
DUMP debitors_per_bank;

-- Charger les données du fichier bank.csv
data = LOAD 'bank.csv' USING PigStorage(';') AS (banque:chararray, client:chararray, versement:int, retrait:int, annee:int);

-- Regrouper les clients par banque et obtenir une liste distincte des identifiants de clients
clients_bag = DISTINCT FOREACH (GROUP data BY banque) GENERATE group AS banque, data.client AS clients:bag{client_id:chararray};
DUMP clients_bag;

-- Charger les données du fichier bank.csv
data = LOAD 'bank.csv' USING PigStorage(';') AS (banque:chararray, client:chararray, versement:int, retrait:int, annee:int);

-- Regrouper les transactions par client et calculer les montants totaux versés et retirés
transactions_per_client = FOREACH (GROUP data BY client) GENERATE group AS client, SUM(data.versement) AS total_verse, SUM(data.retrait) AS total_retrait;
DUMP transactions_per_client;

-- Charger les données du fichier bank.csv
data = LOAD 'bank.csv' USING PigStorage(';') AS (banque:chararray, client:chararray, versement:int, retrait:int, annee:int);

-- Compter le nombre de fois débiteur et le nombre de fois créditeur pour chaque client
debit_credit_count = FOREACH (GROUP data BY client) GENERATE group AS client, COUNT(FILTER data BY retrait > 0) AS nombre_fois_debiteur, COUNT(FILTER data BY versement > 0) AS nombre_fois_crediteur;
DUMP debit_credit_count;


