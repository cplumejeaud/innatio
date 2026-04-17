# Code pour la lecture et l'import des fichiers exportés du CASD. 

Christine Plumejeaud-Perreau, UMR 7301 Migrinter
premier commit : 15 avril 2026

## Metadata de la base de donnée 

fichier "D:\Data\BDD-backups\imhana\maison-inseedb-imhana2021_v2_15avril2026"

### Commande de création du dump

C:\Program Files\PostgreSQL\14\bin\pg_dump.exe --verbose --host=localhost --port=5432 ****** --format=c --encoding=UTF-8 --no-privileges --no-owner --file D:\Data\BDD-backups\imhana/maison-inseedb-imhana2021_v2_15avril2026 -t "imhana2021.geoepci_demo" -t "imhana2021.nat_epci_wide" -t "imhana2021.indicateurs" -t "imhana2021.fusion_epci" -t "imhana2021.nat_epci" -t "imhana2021.basicfusion_epci" inseedb
Task 'PostgreSQL dump' started at Wed Apr 15 19:17:55 CEST 2026
pg_dump: le dernier OID interne est 16383

### Comment restaurer le dump

1. creer une BDD sous postgres (14 ou plus)
2. Ajouter l'extension postgis
3. Creer le schema imhana

4. dump à restaurer avec pg_restore : https://www.postgresql.org/docs/14/backup-dump.html

### Contenu : tables sauvegardées 


![Vue graphique des tables (produite avec DBEaver)](./inseedb%20-%20imhana2021.png "Diagramme des tables du schema imhana2021")

### indicateurs :
	Tous les indicateurs disponibles et croisées avec la nationalité des résidents (source RP 2021 sur CASD)
	Décrits par leur code, leur libellé et la liste des modalités (en français uniquement)
	alter table indicateurs add CONSTRAINT indicateurs_pk PRIMARY KEY ("indicateur","modalites");
	si indicateur est un croisement AGER_SEXE par exemple, alors code01 = AGER, et code02 = SEXE
	La concaténation de "indicateur"||'.'||"modalites" renvoie sur la colonne indicateur des tables de données

### geoepci_demo : 
	la géometrie des EPCI de la France métropolitaine, en Lambert 93, EPSG = 2154, 
	avec le code SIREN (unit) et le nom de l'unité (NOM)
	et auxquelles sont ajoutées la région d'appartenance (INSEE_REG, NOM_M, NOM_2), 
	et un résumé démographique :
	- Tous : toute la population, quelle que soit la nationalité actuelle ou d'origine
	- etrangers : tous les étrangers présents
	- francaisParAcquisition : tous les naturalisés français
	- immigres : le nombre de ceux nés étrangers à l'étranger
	
### nat_epci_long : 
	liste pour toutes les EPCI et pour les indicateurs et pour toutes les nationalités présentes (195) 
	les comptes de cette nationalité suivant: 
	- Ensemble : immigré ou pas, naturalisé ou pas
	- Etranger : déclarent une nationalité non française au recensement
	- Français par acquisition : nés étrangers mais naturalisés français
	- Français de naissance : nés français à la naissance
	- SecondeGeneration : nés étrangers en France
	- Immigrés : nés étrangers à l'étranger
	
	Liste des indicateurs intégrés : 
	pour les individus [ 'SEXE', 'DIPLR', 'POSP', 'CATPR', 'IRANR',  'LTEXC', 'MODTRANS', 'AGER','STAT', 'STATCONJ', 'TACT', 'IMMI',  'ARRIVR']
	pour les logements ['ACHLR', 'HLML',  'INPER', 'INPOM', 'INPSM', 'NBPIR', 'NPER',  'STOCD', 'SURF', 'TYPL', 'VOIT' ]
	
	La clé primaire : (unit, indicateur,  "NAT2", "anneeRp")
	alter table nat_epci_long add CONSTRAINT nat_epci_long_pk PRIMARY KEY (unit, indicateur,  "NAT2", "anneeRp");
	L'indicateur contient la concaténation du code et de la modalité de l'indicateur séparé par un '.'
	
### nat_epci : 
	Petite table redondante avec nat_epci_long mais qui n'informe que sur la démographie totale (Ensemble) des EPCI par nationalité, 
	et avec les résumés (Tous, etrangers, francaisParAcquisition, et immigres) compris dans NAT2, sans autre indicateur
	La clé primaire : (unit,"NAT2","anneeRp")

### nat_epci_wide : 
	liste pour toutes les EPCI et pour les indicateurs et pour toutes les nationalités présentes (195) mais en ligne cette fois
		les comptes de cette nationalité suivant la categorie ('Ensemble' uniquement pour l'instant)
		- Ensemble : immigré ou pas, naturalisé ou pas
		Liste des indicateurs intégrés : 
		pour les individus [ 'SEXE', 'DIPLR', 'POSP', 'CATPR', 'IRANR',  'LTEXC', 'MODTRANS', 'AGER','STAT', 'STATCONJ', 'TACT', 'IMMI',  'ARRIVR']
		pour les logements ['ACHLR', 'HLML',  'INPER', 'INPOM', 'INPSM', 'NBPIR', 'NPER',  'STOCD', 'SURF', 'TYPL', 'VOIT' ]

	La clé primaire : 	(unit, indicateur,  categorie, "anneeRp")
	alter table nat_epci_wide add CONSTRAINT nat_epci_wide_pk PRIMARY KEY (unit, indicateur,  categorie, "anneeRp");
	L'indicateur contient la concaténation du code et de la modalité de l'indicateur séparé par un '.'

