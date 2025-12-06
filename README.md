# innatio
Prototype conçu et réalisé en 2025 par des étudiants en M2 SPE La Rochelle pour explorer des données du RP 2021

Cette application propose une visualisation interactive des données de population étrangère en France. 
Elle a été développée dans le cadre du <i>Master 2 SPE à La Rochelle, UE Data to Information</i>, en décembre 2025, avec l'aide de <a href="https://gresco.labo.univ-poitiers.fr/membres/araujo-dina/">Dina Santos-Araujo</a>
et sous la responsabilité de <a href="https://migrinter.cnrs.fr/membres/christine-plumejeaud-perreau/">Christine Plumejeaud-Perreau</a>, 
enseignante de l'UE par des étudiants du Master 2 SPE : 
<ul>
  <li>Gustave Jouis,</li>
  <li>Julia Mourgues,</li>
  <li>Aurélie Henry.</li>
</ul>
<br>C'est un <b>prototype</b> visant à démontrer les capacités d'exploration et visualisation de données statistiques avec Python (3.10). 
Il nécessite des améliorations pour une utilisation en production. 
Le code source est disponible sur le [github de l'enseignante](https://github.com/cplumejeaud/innatio), sous licence Affero GPL v3.
                
# Allure de l'application : 
![Interface_Web](./Interface_Web.png)

# Installation

sur Ubuntu Noble, avec Apache2. 

## Installer la base de données inseedb sur postgres

Les données ont été créées avec les scripts `script_BDD.ipynb` et `carte_regions.ipynb` à partir d'un fichier CSV `inat_nat_epci_region_202512010950.csv`
Un fichier avec les géométries EPCI 2023 a été utilisé pour donner une géométrie : ![source](https://www.data.gouv.fr/datasets/referent-contours-des-epci-2023/). 
Accès au fichier en téléchargement ![ici](https://www.data.gouv.fr/datasets/referent-contours-des-epci-2023/#/resources/98fe9de6-f86b-4997-8156-9bd18550f214)
Les scripts comportent **des erreurs et il manque des étapes**, et la conception de la **BDD n'est pas optimisée** au niveau taille. Donc on n'a pas gardé dans github le script SQL de restauration de la base. Dans le schéma poisson_dina les élèves trouvent les deux tables qu'utilise la webapp : `inat_nat_epci_region` et `nat_etrg_par_epci`

<table><tr>
<td>[inat_nat_epci_region]("savoie - poisson_aurelie - inat_nat_epci_region.png")</td>
<td>[nat_etrg_par_epci]("savoie - poisson_dina - nat_etrg_par_epci.png")</td>
</tr></table>

1) Créer l'utilisateur dédié
`sudo -u postgres createuser -D -P insee`
Saisir le mot de passe pour le nouveau rôle : insee2025


2) Créer la base de données et ajouter les extensions dessus

```sh
sudo -u postgres createdb --encoding=UTF8  --owner=insee inseedb
sudo -u postgres psql -U postgres -d inseedb -c "create extension postgis"
```

3) installer les données dans le schéma

```sh
sudo -u postgres psql -U postgres -d inseedb -c "create schema poisson_dina"
sudo -u postgres psql -U postgres -d inseedb -f /home/cperreau/insee/poisson_dina-savoie-202512041827.sql
sudo -u postgres psql -U insee -d inseedb -p 5432 -c "select postgis_version()"
```

Echoue car pas encore les droits d'accès

4) Droits d'accès pour votre utilisateur

`sudo -u postgres psql -U postgres -d inseedb -p 5432`

```sql
GRANT create, USAGE ON SCHEMA poisson_dina TO insee;
GRANT create,USAGE ON SCHEMA public to insee;
GRANT ALL ON ALL TABLES IN SCHEMA poisson_dina, public TO insee;
grant CONNECT on database inseedb to insee;
grant ALL on all sequences in schema poisson_dina, public to insee;
```

sudo -u postgres psql -U insee -d inseedb -p 5432 -c "select postgis_version()"
```sql
 3.6 USE_GEOS=1 USE_PROJ=1 USE_STATS=1
```

Le script SQL à faire sur Windows qui doit préparer la base de données sur le serveur est conservé (pour les étudiants)
```sql
create extension postgis;
create schema poisson_dina;
CREATE ROLE insee NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN NOREPLICATION NOBYPASSRLS 
PASSWORD 'insee2025';
GRANT ALL ON SCHEMA public TO insee;
GRANT ALL ON SCHEMA poisson_dina TO insee;
```

L'URL de connexion à la base en local sera donc : `postgresql://insee:insee2025@localhost:5432/inseedb`

Pour vérifier que votre accès à la base marche bien : 
```sql
SELECT 
            "EPCI",
            "nom_epci",
            "NAT_rec3" AS "Nationalite",
            "total_s",
            "part_etrg_epci",
            "geometry"
        FROM poisson_dina.nat_etrg_par_epci;

-- Aurélie HENRY
-- Voilà le SQL (de mémoire) qui a permis de jointer le csv de data.gouv téléchargé dans DBeaver avec le fichier inat_nat... avec la clé SIREN/EPCI pour ajouter la colonne nom dans inat_nat...
update inat_nat_epci_region
set SIREN = e.SIREN, nom =e.nom from EPCI_name e
where inat_nat_epci_region.EPCI = e.SIREN;
```

## Installer la webapp

Si nécessaire, le fichier wsgi peut être édité avec *vi* ou *nano* sous Linux. Voir ce site https://www.linuxtricks.fr/wiki/guide-de-sur-vi-utilisation-de-vi

`vi innatio.wsgi`
```py
import sys
sys.path.insert(0, '/var/www/insee')

from app import app as application
```

Installation d'un environnement virtuel pour python 3.10 dans ce répertoire
`cd ~/insee/webapp`
`python3.10  -m venv py310-venv`

Environnement virtuel dans : 
- /home/cperreau/insee/webapp/py310-venv

**entrer**
`source py310-venv/bin/activate`

**installer des packages listés dans un fichier requirements_venv.txt**
`pip3 install -r ../requirements_20251206.txt`

**intaller le module WSGI** / Successfully installed mod_wsgi-5.0.2
`pip install mod_wsgi --use-pep517`

**sortir**
`deactivate`

## Apache2

Supprimer mes Ctrl^M génants de Windows parfois
```sh
for fic in $(find /home/cperreau/insee/webapp -type f -name "*.py"); do sudo dos2unix $fic; done
```

Attention, il faut qu'Apache2 (user :www-data) ait accès à votre environnement virtuel (en lecture et exécution, r+x)
```sh
sudo chown :www-data /home/cperreau/insee/webapp/ -R
sudo chmod 755 /home/cperreau/insee/webapp/ -R
sudo chmod 755 /home/cperreau
```
Lier les sources à un répertoire fictif apache
`sudo ln -s  /home/cperreau/insee/webapp/ /var/www/innatio`

DNS : innatio.plumegeo.fr
créer un mapping sur votre fournisseur de DNS : CNAME avec romarin.huma-num.fr.

Une vérification s'impose : pour apache2, quel WSGI va s'exécuter ?

`source py310-venv/bin/activate`
`mod_wsgi-express module-config`
```sh
LoadModule wsgi_module "/home/cperreau/insee/webapp/py310-venv/lib/python3.10/site-packages/mod_wsgi/server/mod_wsgi-py310.cpython-310-x86_64-linux-gnu.so"
WSGIPythonHome "/home/cperreau/insee/webapp/py310-venv"
```

Il faut reporter ces infos dans le fichier de config **innatio.conf** ci-dessous

`sudo vi /etc/apache2/sites-available/innatio.conf`
```sh
<VirtualHost *:80>
    ServerName innatio.plumegeo.fr
    DocumentRoot /var/www/innatio

    LoadModule wsgi_module "/home/cperreau/insee/webapp/py310-venv/lib/python3.10/site-packages/mod_wsgi/server/mod_wsgi-py310.cpython-310-x86_64-linux-gnu.so"
    WSGIDaemonProcess innatio python-home="/home/cperreau/insee/webapp/py310-venv"
    WSGIProcessGroup innatio

    WSGIApplicationGroup %{GLOBAL}

    WSGIScriptAlias / /var/www/innatio/innatio.wsgi

    <Directory /var/www/innatio>
        Require all granted
    </Directory>

</VirtualHost>

```

`sudo a2ensite innatio` Pour démarrer la webapp
`sudo a2dissite innatio` Pour retirer la webapp

`sudo systemctl reload apache2` pour recharger la config et le code de la Webapp
`sudo systemctl restart apache2.service` pour stopper/redémarrer apache2

`sudo systemctl status apache2.service` : état du service Apache2

`sudo vi /var/log/apache2/error.log` : debugger et regarder les traces de la webapp
