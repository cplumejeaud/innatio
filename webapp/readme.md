# code de la webapp 

  - templates/ #Fichiers HTML de la webapp, templates de Flask
  - py310-venv/ #Environnement virtuel pour l'appli
    
  - data_etrangers.geojson : les données précalculées pour la carte de droite centrée sur une région, et les epci

  - app.py : point d'entrée dans la webapp qui appelle le template index.html, route /
  - histogrammes.py : partie qui calcule l'histogramme en bas à droite
  - carte_nationalites_par_epci.py : /cartes , partie qui calcule la carte en haut à gauche, sur la France entière
  - mon_graphique.py : partie qui calcule la carte en haut à droite, centrée sur une region, route /app_carte_region
  - app_carte_region.py : idem. Mais c'est le code de mon_graphique qui s'exécute.

  - requirements.txt : les librairies à installer
  - innatio.wsgi : le fichier wsgi pour apache2
