import requests
from bs4 import BeautifulSoup
#& C:/Tools/Python/python310/python.exe -m pip install bs4
#& C:\Tools\Python\python310\python.exe -m pip install --upgrade pip
#& C:/Tools/Python/python310/python.exe -m pip install regex

import re
import pandas as pd

def get_population_from_siren(siren):
    # 1. Recherche de la page contenant le SIREN
    HEADERS = {
    "User-Agent": "extract_wikipedia.py/1.0 (contact: cplumejeaud@gmail.com)"
    }
    search_url = "https://fr.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "list": "search",
        "srsearch": siren,
        "format": "json"
    }
    r = requests.get(search_url, params=params, headers=HEADERS)
    print(r.url)  # Affiche l'URL de la requête pour vérification
    print("Status:", r.status_code)
    print("Content:", r.text[:500])
    data = r.json()

    if not data["query"]["search"]:
        return None, "Aucune page trouvée pour ce SIREN"

    foundinfobox = False
    print ("Pages trouvées :", len(data["query"]["search"]))
    i = 0
    infobox = "";
    while (i < len(data["query"]["search"]) and not foundinfobox):
    # On prend la première page trouvée
        page_title = data["query"]["search"][i]["title"]

        # 2. Récupération du contenu HTML de la page
        page_url = f"https://fr.wikipedia.org/wiki/{page_title.replace(' ', '_')}"
        #page_url = "https://fr.wikipedia.org/wiki/Communaut%C3%A9_de_communes_Vaison_Ventoux"
        html = requests.get(page_url, headers=HEADERS).text
        soup = BeautifulSoup(html, "html.parser")
        

        # 3. Extraction de la population dans l’infobox
        #infobox = soup.find("table", {"class": "infobox"})
        infobox = soup.select_one("table.infobox")
        #infobox = soup.find("table", class_="infobox")
        
        
        if not infobox:
            #return page_title, "Infobox introuvable"
            print(f"Infobox introuvable pour la page '{page_title}', passage à la page suivante...")
            i=i+1
        else:
            foundinfobox = True
    
    #page_url = f"https://fr.wikipedia.org/wiki/Communaut%C3%A9_de_communes_C%C5%93ur_de_Garonne"
    #page_url = f"https://fr.wikipedia.org/wiki/Communaut%C3%A9_de_communes_Vaison_Ventoux"
    #html = requests.get(page_url).text
    #soup = BeautifulSoup(html, "html.parser")

    # 3. Extraction de la population dans l’infobox
    
    #infobox = soup.select_one("table.infobox")
    
    # Recherche d'une ligne contenant "Population"
    for row in infobox.find_all("tr"):
        header = row.find("th")
        if header and "Population" in header.text:
            td = row.find("td")
            if td:
                text = td.get_text(" ", strip=True)

                # Extraire uniquement le nombre avant "hab."
                match = re.search(r"(\d[\d\s ]*)\s*hab", text)
                if match:
                    # Nettoyage du texte pour ne garder que les chiffres
                    #pop = re.sub(r"\D+", "", td.text)
                    pop = re.sub(r"\D", "", match.group(1))
                    return page_title, pop              
                
    return page_title, "Population non trouvée"


# Exemple d'utilisation :
siren = "200068815"  # CC Cœur de Garonne
#siren = "248400335"  # CC Vaison Ventoux
title, population = get_population_from_siren(siren)
print("Page :", title)
print("Population :", population)

excel_file = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\selections_terrains_communes.xlsx"


bilan_demographie_doublons = pd.read_excel(excel_file, sheet_name='bilan_demographie_doublons')
for index, row in bilan_demographie_doublons.iterrows():
    siren = row['unit']
    title, population = get_population_from_siren(siren)
    print(f"SIREN: {siren}, Page: {title}, Population: {population}")   
    bilan_demographie_doublons.at[index, 'Wikipedia_Page'] = title
    bilan_demographie_doublons.at[index, 'Population'] = population
    
bilan_demographie_doublons.to_excel(r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\bilan_demographie_doublons_avec_population.xlsx", index=False)