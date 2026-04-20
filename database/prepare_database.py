import pandas as pd
import geopandas as gpd
import json
from shapely.geometry import shape
import folium
from sqlalchemy import create_engine
import logging
import os
import pandas.io.sql as sql
from sqlalchemy import create_engine, text
import os
pd.options.mode.chained_assignment = None  # default='warn'
import platform

prgpath = '/home/cperreau/insee/database'
datapath = "/home/cperreau/imhana/export_CASD_ergonomiques/2026.01.22/EPCI/"
dbname = 'inseedb'
dbuser = '*********'
dbpassword = '**********'

if platform.system() == 'Windows':
    prgpath = 'C:\Travail\Enseignement\Cours_M2_python\\2025\Projet_INSEE\Insee\database'
    datapath = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\EPCI\\"
    dbname = 'inseedb'
    dbuser = 'postgres'
    dbpassword = 'postgres'

os.chdir(prgpath)

connectString = f'postgresql://{dbuser}:{dbpassword}@localhost:5432/{dbname}'
print(connectString)

epcisuffix = "_EPCI_2026.01.22.csv"


EPT_Paris = ['200057867','200057875','200057941','200057966','200057974','200057982','200057990','200058006','200058014','200058097','200058790']
EPCI_fictives = ['HORS__GFP', 'RESTANT__GFP', 'fictive_200027399', 'fictive_242320034', 'fictive_242320059', 'fictive_244701389']
STRANGERS = ['Etranger', 'Français par acquisition']

nationalites_speciales = ['Tous', 'etrangers', 'francaisParAcquisition', 'immigres']
liste_natio = pd.read_csv('liste_nationalites.csv')['nationalites'].tolist()
FRENCH_DOMTOM = pd.read_csv('FRENCH_DOMTOM.csv', sep=';', encoding='utf-8', dtype={'FRENCH_DOMTOM':str})['FRENCH_DOMTOM'].tolist()

doublons_CC_EPCI = pd.read_csv('doublons_CC_EPCI.csv', sep=';', encoding='utf-8', dtype={'doublons_CC_EPCI':str})['doublons_CC_EPCI'].tolist()
doublons_CA_CU_EPCI = pd.read_csv('doublons_CA_CU_EPCI.csv', sep=';', encoding='utf-8', dtype={'doublons_CA_CU_EPCI':str})['doublons_CA_CU_EPCI'].tolist()
print(doublons_CC_EPCI)
print(doublons_CA_CU_EPCI)

doublons_COM = pd.read_csv('doublons_COMMUNES.csv', sep=';', encoding='utf-8', dtype={'doublons_COMMUNES':str})['doublons_COMMUNES'].tolist()
print(doublons_COM)

CODESEP = '.' #séparateur entre le code et les modalité d'un indicateur (SEXE.Féminin par exemple)

demographieCSVtype={"total_s": int, "unit": "string", "NOM": "string", "NAT2": "string"}



def process_NAT_EPCI_first():
    '''
    appelé par fusion_EPCI_NAT2 pour initialiser la table nat_epci_long avec les totaux ('Ensemble') denationalité par unité 
    Doit être appelé en premier car définit les colonnes anneeRp, indicateur, indicateurCode, indicateurMode
    La nationalité NAT2 est recodée : elle prend pour valeur NAT (nationalité actuelle) ou NATN si l'individu a été naturalisé français. 
    Ce qui fait que les comptes Ensemble définissent la somme des personnes d'origine d'une nationalité, qu'ils soient encore de cette nationalité, ou bien aient été naturalisés français. 
    '''
    
    nat_epci_file = datapath+"NAT"+epcisuffix
    #nat_epci_file = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\EPCI\NAT_EPCI_2026.01.22.csv"

    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)
    
    demo01 = demographie_etrangers.query(" NAT2 not in @nationalites_speciales  and not(unit in @doublons_CC_EPCI) and not(unit in @doublons_CA_CU_EPCI) ").pivot(index=['unit', 'NOM'], columns='NAT2', values='total_s')
    demo02 = demographie_etrangers.query(" NAT2 not in @nationalites_speciales and (unit in @doublons_CC_EPCI) ").pivot_table(index=['unit', 'NOM'], columns='NAT2', values='total_s',aggfunc="max") 
    demo03 = demographie_etrangers.query(" NAT2 not in @nationalites_speciales and (unit in @doublons_CA_CU_EPCI) ").pivot_table(index=['unit', 'NOM'], columns='NAT2', values='total_s',aggfunc="sum") 

    frames = [demo01, demo02, demo03]

    result = pd.concat(frames)
    result.fillna(0, inplace=True)
    result.rename_axis(columns=None, inplace=True)
    result.reset_index(inplace=True)

    print(result.columns)
    print(result.shape) #(1224, 6)

    print(result)
    database = result
    database = result.melt(id_vars=['unit', 'NOM'], value_vars=liste_natio, var_name='NAT2', value_name='total_s')
    database = database.astype(dtype = {'unit': 'string', 'NOM': 'string', 'NAT2': 'string', 'total_s': 'int'})
    database.rename(columns={'total_s':'Ensemble'}, inplace=True)

    print(database.shape) #(243576, 4)
    print(database.columns) #['unit', 'NOM', 'NAT2', 'Ensemble']
    print(database.dtypes)
    # unit               string
    # NOM                string
    # NAT2               string
    # Ensemble     int32
    
    database['anneeRp']  = 2021
    database['indicateur']  = 'NAT2'+CODESEP
    database['indicateurCode']  = 'NAT2'
    database['indicateurMode']  = ''
    #Retirer la colonne NOM
    database = database[['unit', 'anneeRp', 'indicateur', 'indicateurCode', 'indicateurMode', 'NAT2', 'Ensemble']]
    
    return  database

def process_INAT_EPCI():
    '''
    appelé par fusion_EPCI_NAT2 pour initialiser la table nat_epci_long
    '''
    csvtype = demographieCSVtype
    csvtype['INAT_BIS'] = 'string'
    nat_epci_file = datapath+"INAT_NAT"+epcisuffix
    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)
    
    demo01 = demographie_etrangers.query(" NAT2 not in @nationalites_speciales  and not(unit in @doublons_CC_EPCI) and not(unit in @doublons_CA_CU_EPCI) ").pivot(index=['unit', 'NOM', 'NAT2'], columns='INAT_BIS', values='total_s')
    demo02 = demographie_etrangers.query(" NAT2 not in @nationalites_speciales and (unit in @doublons_CC_EPCI) ").pivot_table(index=['unit', 'NOM', 'NAT2'], columns='INAT_BIS', values='total_s',aggfunc="max") 
    demo03 = demographie_etrangers.query(" NAT2 not in @nationalites_speciales and (unit in @doublons_CA_CU_EPCI) ").pivot_table(index=['unit', 'NOM', 'NAT2'], columns='INAT_BIS', values='total_s',aggfunc="sum") 
        
    frames = [demo01, demo02, demo03]
    result = pd.concat(frames)

    result.fillna(0, inplace=True)
    result.rename_axis(columns=None, inplace=True)
    result.reset_index(inplace=True)

    #Rajouter une colonne Français de naissance  
    result['Français de naissance'] = result[FRENCH_DOMTOM].sum(axis=1)

    #Réordonner les colonnes et retirer NOM
    result = result[['unit', 'NAT2', 'Etranger', 'Français par acquisition', 'Français de naissance'] + [col for col in result.columns if col not in ['unit', 'NOM', 'NAT2', 'Etranger', 'Français par acquisition', 'Français de naissance']]]

    print(result.columns)
    print(result.shape) #(96033, 18)
    return result

def process_GEN2_NAT_EPCI():
    '''
    appelé par fusion_EPCI_NAT2 pour initialiser la table nat_epci_long
    '''

    nat_epci_file = datapath+"GEN2_NAT"+epcisuffix
    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)

    demo01 = demographie_etrangers.query(" NAT2 not in @nationalites_speciales  and not(unit in @doublons_CC_EPCI) and not(unit in @doublons_CA_CU_EPCI) ").pivot(index=['unit', 'NOM', 'NAT2'], columns='GENERATION2', values='total_s')
    demo02 = demographie_etrangers.query(" NAT2 not in @nationalites_speciales and (unit in @doublons_CC_EPCI) ").pivot_table(index=['unit', 'NOM', 'NAT2'], columns='GENERATION2', values='total_s',aggfunc="max") 
    demo03 = demographie_etrangers.query(" NAT2 not in @nationalites_speciales and (unit in @doublons_CA_CU_EPCI) ").pivot_table(index=['unit', 'NOM', 'NAT2'], columns='GENERATION2', values='total_s',aggfunc="sum") 
        
    frames = [demo01, demo02, demo03]
    result = pd.concat(frames)

    result.fillna(0, inplace=True)
    result.rename_axis(columns=None, inplace=True)
    result.reset_index(inplace=True)

    # Renommer True en SecondeGeneration - étranger né en France 
    # SecondeGeneration: les étrangers (INAT = 21) ou francais naturalisés (INAT = 12) nés en France (donc non immigrés)
    # GENERATION2 = ifelse(INAT!=11 & IMMI==2, TRUE, FALSE)
    result.rename(columns={True:'SecondeGeneration', False:'PremiereGeneration'}, inplace=True)

    # retirer NOM des colonnes
    result = result[['unit', 'NAT2', 'SecondeGeneration', 'PremiereGeneration'] ]

    print(result.columns)
    print(result.shape) #(96033, 18)
    return result

def process_IMMI_NAT_EPCI():
    '''
    appelé par fusion_EPCI_NAT2 pour initialiser la table nat_epci_long
    '''

    nat_epci_file = datapath+"IMMI_NAT"+epcisuffix
    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)

    demo01 = demographie_etrangers.query(" NAT2 not in @nationalites_speciales  and not(unit in @doublons_CC_EPCI) and not(unit in @doublons_CA_CU_EPCI) ").pivot(index=['unit', 'NOM', 'NAT2'], columns='IMMI', values='total_s')
    demo02 = demographie_etrangers.query(" NAT2 not in @nationalites_speciales and (unit in @doublons_CC_EPCI) ").pivot_table(index=['unit', 'NOM', 'NAT2'], columns='IMMI', values='total_s',aggfunc="max") 
    demo03 = demographie_etrangers.query(" NAT2 not in @nationalites_speciales and (unit in @doublons_CA_CU_EPCI) ").pivot_table(index=['unit', 'NOM', 'NAT2'], columns='IMMI', values='total_s',aggfunc="sum") 
        
    frames = [demo01, demo02, demo03]
    result = pd.concat(frames)

    result.fillna(0, inplace=True)
    result.rename_axis(columns=None, inplace=True)
    result.reset_index(inplace=True)
    
    # retirer NOM des colonnes
    result = result[['unit', 'NAT2', 'Immigrés', 'Non immigrés'] ]

    print(result.columns)
    print(result.shape) #(96033, 18)
    return result

def process_niveau1_EPCI_first(variable = 'SEXE'):
    demographieCSVtype[variable] = 'string'
    
    nat_epci_file = datapath+variable+"_NAT"+epcisuffix
    #nat_epci_file = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\EPCI\NAT_EPCI_2026.01.22.csv"

    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)

    # passer en large pour gérer les doublons
    demo01 = demographie_etrangers.query("NAT2 not in @nationalites_speciales  and not(unit in @doublons_CC_EPCI) and not(unit in @doublons_CA_CU_EPCI) ").pivot(index=['unit', 'NOM', variable], columns='NAT2', values='total_s')
    demo02 = demographie_etrangers.query("NAT2 not in @nationalites_speciales and (unit in @doublons_CC_EPCI) ").pivot_table(index=['unit', 'NOM', variable], columns='NAT2', values='total_s',aggfunc="max") 
    demo03 = demographie_etrangers.query("NAT2 not in @nationalites_speciales and (unit in @doublons_CA_CU_EPCI) ").pivot_table(index=['unit', 'NOM', variable], columns='NAT2', values='total_s',aggfunc="sum") 

    frames = [demo01, demo02, demo03]

    result = pd.concat(frames)
    result.fillna(0, inplace=True)
    result.rename_axis(columns=None, inplace=True)
    result.reset_index(inplace=True)

    #Repasser en long
    liste_natio_adaptee = liste_natio
    if len(demographie_etrangers.NAT2.unique()) != len(liste_natio) + 4 :  
        #199 = 195 nationalités + 4 nationalites_speciales, mais dans la table logement, moins de nationalités sont représentées (193)
        #199 * 4 octets pour un int selon le type de données, soit 796 octets, ce qui est cohérent avec les 103241 lignes et 10 colonnes du test
        #"The following 'value_vars' are not present in the DataFrame: ['Marshallais', 'Tuvaluans']" pour logements
        liste_natio_adaptee = demographie_etrangers.query("NAT2 not in @nationalites_speciales").NAT2.unique().tolist()
        print(f"Nombre de nationalites {len(liste_natio_adaptee)} avec la variable {variable} ")

    database = result.melt(id_vars=['unit', 'NOM', variable], value_vars=liste_natio_adaptee, var_name='NAT2', value_name='total_s')
    database = database.astype(dtype = {'unit': 'string', 'NOM': 'string', 'NAT2': 'string', 'total_s': 'int'})
    database.rename(columns={variable : 'indicateurMode', 'total_s':'Ensemble'}, inplace=True)
    database['anneeRp']  = 2021
    database['indicateurCode']  = variable
    
    if variable :
        database['indicateur'] = database['indicateurCode']+CODESEP+database['indicateurMode']
    
    print(database.shape) #(477360, 5)
    print(database.columns) #['unit', 'NOM', 'indicateurMode', 'NAT2', 'Ensemble']
    #print(database.dtypes)

    return database[['unit', 'NAT2', 'anneeRp', 'indicateur', 'indicateurCode', 'indicateurMode',  'Ensemble' ]]
    
def process_niveau1_EPCI_correspondances(croix= 'GEN2', variable='SEXE') :
    demographieCSVtype[variable] = 'string'

    
    # variable = 'SEXE'
    # croix = 'GEN2'
    correspondances = {'INAT':'INAT_BIS', 'GEN2' : 'GENERATION2', 'IMMI' : 'IMMI'}

    nat_epci_file = datapath+variable+"_"+croix+"_NAT"+epcisuffix
    #nat_epci_file = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\EPCI\NAT_EPCI_2026.01.22.csv"

    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)

    #unit == '248500191' and  
    demo01 = demographie_etrangers.query("NAT2 not in @nationalites_speciales  and not(unit in @doublons_CC_EPCI) and not(unit in @doublons_CA_CU_EPCI) ").pivot(index=['unit', 'NOM', 'NAT2', variable], columns=correspondances[croix], values='total_s')
    demo02 = demographie_etrangers.query("NAT2 not in @nationalites_speciales and (unit in @doublons_CC_EPCI) ").pivot_table(index=['unit', 'NOM', 'NAT2', variable], columns=correspondances[croix], values='total_s',aggfunc="max") 
    demo03 = demographie_etrangers.query("NAT2 not in @nationalites_speciales and (unit in @doublons_CA_CU_EPCI) ").pivot_table(index=['unit', 'NOM', 'NAT2', variable], columns=correspondances[croix], values='total_s',aggfunc="sum") 

    frames = [demo01, demo02, demo03]
    result = pd.concat(frames)

    result.fillna(0, inplace=True)
    result.rename_axis(columns=None, inplace=True)
    result.reset_index(inplace=True)

    # renommer la colonne variable par indicateurMode 
    result.rename(columns={variable : 'indicateurMode'}, inplace=True)

    if (croix == 'INAT') :
        #Rajouter une colonne Français de naissance
        result['Français de naissance'] = result[FRENCH_DOMTOM].sum(axis=1)
        #Réordonner les colonnes, et retirer NOM
        result = result[['unit', 'NAT2', 'indicateurMode', 'Etranger', 'Français par acquisition', 'Français de naissance'] + [col for col in result.columns if col not in ['unit', 'NOM', 'NAT2', 'indicateurMode', 'Etranger', 'Français par acquisition', 'Français de naissance']]]
    if (croix ==  'GEN2') : 
        #Attention, petite cuisine malhonnète ici : Immigrés vaut normalement 'PremiereGeneration'
        result.rename(columns={True:'SecondeGeneration', False:'Immigrés'}, inplace=True)
        # retirer NOM des colonnes
        result = result[['unit', 'NAT2', 'indicateurMode', 'SecondeGeneration', 'Immigrés'] ]#PremiereGeneration 
        # bricolage car IMMI n'est pas croisé avec les variables de niveau 1 et Immigrés = PremiereGeneration
    if (croix ==  'IMMI') : 
        # retirer NOM des colonnes
        result = result[['unit', 'NAT2', 'indicateurMode', 'Immigrés', 'Non immigrés'] ]
        
    print(result.columns)
    print(result.shape) #(157960, 18)

    return result

def process_NAT_EPCI_wide():
    nat_epci_file = datapath+"NAT"+epcisuffix
    #nat_epci_file = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\EPCI\NAT_EPCI_2026.01.22.csv"

    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)
    
    demo01 = demographie_etrangers.query("not(unit in @EPCI_fictives) and not(unit in @doublons_CC_EPCI) and not(unit in @doublons_CA_CU_EPCI) ").pivot(index=['unit', 'NOM'], columns='NAT2', values='total_s')
    demo02 = demographie_etrangers.query(" (unit in @doublons_CC_EPCI) ").pivot_table(index=['unit', 'NOM'], columns='NAT2', values='total_s',aggfunc="max") 
    demo03 = demographie_etrangers.query(" (unit in @doublons_CA_CU_EPCI) ").pivot_table(index=['unit', 'NOM'], columns='NAT2', values='total_s',aggfunc="sum") 

    frames = [demo01, demo02, demo03]

    result = pd.concat(frames)
    result.fillna(0, inplace=True)
    result.rename_axis(columns=None, inplace=True)
    result.reset_index(inplace=True)

    print(result.columns)
    print(result.shape) #(1224, 6)

    database_wide = result[['unit']]
    database_wide['anneeRp'] =  2021
    database_wide['indicateur'] =  'NAT2'+CODESEP #servira de PK avec unit, et anneeRp et permettra de faire le lien avec les autres indicateurs
    database_wide['indicateurCode'] =  'NAT2' 
    database_wide['indicateurMode'] =  '' 
    database_wide['categorie'] = 'Ensemble' #	Etrangers, Français par acquisition,  SecondeGénération, Immigrés	

    database_wide = pd.merge(database_wide, result, on=['unit'], how='left') 
    print(database_wide.shape)
    #(1224, 206) avec les 4 colonnes de unit, anneeRp, indicateur, indicateurCode, indicateurMode, categorie, et les 197 colonnes de nationalités, 'Tous' puis 'etrangers', 'francaisParAcquisition', 'immigres')

    return  database_wide


def process_niveau1_EPCI_wide(variable = 'SEXE'):
    nat_epci_file = datapath+variable+"_NAT"+epcisuffix
    #nat_epci_file = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\EPCI\NAT_EPCI_2026.01.22.csv"

    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)

    #unit == '248500191' and  
    demo01 = demographie_etrangers.query("not(unit in @EPCI_fictives) and not(unit in @doublons_CC_EPCI) and not(unit in @doublons_CA_CU_EPCI) ").pivot(index=['unit', 'NOM', variable], columns='NAT2', values='total_s')
    demo02 = demographie_etrangers.query(" (unit in @doublons_CC_EPCI) ").pivot_table(index=['unit', 'NOM', variable], columns='NAT2', values='total_s',aggfunc="max") 
    demo03 = demographie_etrangers.query(" (unit in @doublons_CA_CU_EPCI) ").pivot_table(index=['unit', 'NOM', variable], columns='NAT2', values='total_s',aggfunc="sum") 


    frames = [demo01, demo02, demo03]

    result = pd.concat(frames)
    result.fillna(0, inplace=True)
    result.rename_axis(columns=None, inplace=True)
    result.reset_index(inplace=True)

    print(result.columns)
    print(result.shape) #(1224, 6)

    database_wide = result[['unit']]
    database_wide['anneeRp'] =  2021
    database_wide['indicateurCode'] =  variable 
    database_wide['indicateurMode'] =  result[[variable]]
    
    database_wide['indicateur'] =  database_wide['indicateur']+CODESEP+database_wide['indicateurMode']
    #servira de PK avec unit, et anneeRp et permettra de faire le lien avec les autres indicateurs

    database_wide['categorie'] = 'Ensemble' #	Etrangers, Français par acquisition,  SecondeGénération, Immigrés	

    result['indicateurMode']=  result[[variable]]
    database_wide = pd.merge(database_wide, result, on=['unit', 'indicateurMode'], how='left') 
    database_wide.drop(columns=['NOM', variable], inplace=True)
    print(database_wide.shape)
    #(1224, 206) avec les 4 colonnes de unit, anneeRp, indicateur, indicateurCode, indicateurMode, categorie, et les 197 colonnes de nationalités, 'Tous' puis 'etrangers', 'francaisParAcquisition', 'immigres')

    return  database_wide


def process_niveau1_EPCI(variable = 'SEXE'):
    '''
    Calcule un table qui garde en ligne les unit, NAT2 et pivote les différentes modalités de la variable sur la ligne 
    utilisé ensuite pour rajouter les colonnes de cet indicateur sur une ligne correspondant à une nationalité et une unité avec `add_columns_to_nat_epci`
    (repose sur l'existence de la table nat_epci qui décrit l'ensemble des indicateurs pour 'Ensemble')
    les tables créés (autant que de variables, soit 24) doivent ensuite être supprimées.
    DEPRECATED 
    '''
    nat_epci_file = datapath+variable+"_NAT"+epcisuffix
    #nat_epci_file = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\EPCI\NAT_EPCI_2026.01.22.csv"

    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)

    #unit == '248500191' and  
    demo01 = demographie_etrangers.query("not(unit in @EPCI_fictives) and not(unit in @doublons_CC_EPCI) and not(unit in @doublons_CA_CU_EPCI) ").pivot(index=['unit', 'NOM', variable], columns='NAT2', values='total_s')
    demo02 = demographie_etrangers.query(" (unit in @doublons_CC_EPCI) ").pivot_table(index=['unit', 'NOM', variable], columns='NAT2', values='total_s',aggfunc="max") 
    demo03 = demographie_etrangers.query(" (unit in @doublons_CA_CU_EPCI) ").pivot_table(index=['unit', 'NOM', variable], columns='NAT2', values='total_s',aggfunc="sum") 


    frames = [demo01, demo02, demo03]

    result = pd.concat(frames)
    result.fillna(0, inplace=True)
    result.rename_axis(columns=None, inplace=True)
    result.reset_index(inplace=True)

    print(result.columns)
    print(result.shape) #(1224, 6)

    print(result)
    database = result
    #demographie_etrangers.query("unit == '248500191'").NAT2.unique().tolist() à la place liste_natio
    database = result.melt(id_vars=['unit', 'NOM', variable], value_vars=liste_natio+nationalites_speciales, var_name='NAT2', value_name='total_s')
    database = database.astype(dtype = {'unit': 'string', 'NOM': 'string', 'NAT2': 'string', 'total_s': 'int'})

    print(database.shape) #(243576, 4)
    print(database.columns) #['unit', 'NOM', 'NAT2', 'POPULATION_NAT2']
    print(database.dtypes)
    database

    database = database.pivot(index=['unit', 'NOM', 'NAT2'], columns=variable, values='total_s')
    database.rename_axis(columns=None, inplace=True)
    database.reset_index(inplace=True)
    print(database.shape) #(243576, 5) 
    print(database.columns) #['unit', 'NOM', 'NAT2', 'Féminin', 'Masculin'] 
    print(database.dtypes)
    return database


def save_to_database(df, table_name, schema_name='imhana'):

    engine = create_engine(connectString, connect_args={'options': '-csearch_path={}'.format('imhana,public')})
    ORM_conn=engine.connect()
    df.to_sql(table_name, con=ORM_conn , schema=schema_name, if_exists='replace', index=False)
    ORM_conn.commit()
    ORM_conn.close() 
        
    # engine = create_engine('postgresql://postgres:postgres@localhost:5432/inseedb')
    # df.to_sql(table_name, con=engine , schema=schema_name, if_exists='replace', index=False)
    # engine.dispose()

def append_to_database(df, table_name, schema_name='imhana'):
    engine = create_engine(connectString, connect_args={'options': '-csearch_path={}'.format('imhana,public')})
    df.to_sql(table_name, con=engine , schema=schema_name, if_exists='append', index=False)
    engine.dispose()

    



def add_columns_to_nat_epci(df, variable = 'sexe'):
    '''
    pour rajouter la colonne d'un indicateur sur une ligne correspondant à une nationalité
    DEPRECATED (repose sur l'existence de la table nat_epci qui décrit l'ensemble des indicateurs pour 'Ensemble')
    '''
    engine = create_engine(connectString, connect_args={'options': '-csearch_path={}'.format('imhana,public')})
    ORM_conn=engine.connect()
    sql_query = ''
    attlist = []
    for col in df.columns[3:].tolist():
        sql_query = sql_query+ f"alter table nat_epci add column \"{col}\" int; "
        attlist.append(f" \"{col}\" = nat_epci_{variable}.\"{col}\" ")
    #print(attlist)

    sql_query = sql_query + """
        update nat_epci set """ + ", ".join(attlist) + """
        from nat_epci_{variable} 
        where nat_epci.unit = nat_epci_{variable}.unit and nat_epci."NAT2" = nat_epci_{variable}."NAT2" ;""".format(variable=variable)

    sql_query = sql_query + f"""drop table nat_epci_{variable};"""
    print(sql_query)
    ORM_conn.execute(text(sql_query))

    ORM_conn.commit()
    ORM_conn.close() 
    


def make_dico_variables(varname, modalites_variables, onglet = 'RP_individus_principale'):
    #Lire C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\Dico_variables_RP_v2.xlsx

    dico_variables_file = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\Dico_variables_RP_v2.xlsx"
    dico_variables = pd.read_excel(dico_variables_file, sheet_name=onglet, skiprows=3)
    
    #modalites_variables = modalitesList #database.columns[3:].tolist()

    df = pd.DataFrame({'indicateur' : varname, 'modalites': modalites_variables})
    df['code01'] = df.indicateur.apply(lambda x: x.split('_')[0])
    df['modalite01'] = df.modalites.apply(lambda x: x.split('_')[0])
    indicateurs = pd.merge(df, dico_variables [['Nom de la variable', 'Libellé']], left_on='code01', right_on='Nom de la variable', how='inner')
    indicateurs.rename(columns={'Libellé':'libelle01'}, inplace=True)
    indicateurs.drop(columns=['Nom de la variable'], inplace=True)


    if len(varname.split('_')) == 2 : 
        df['code02'] = df.indicateur.apply(lambda x: x.split('_')[1])
        df['modalite02'] = df.modalites.apply(lambda x: x.split('_')[1])
        indicateurs = pd.merge(indicateurs, dico_variables [['Nom de la variable', 'Libellé']], left_on='code02', right_on='Nom de la variable', how='inner')
        indicateurs.rename(columns={'Libellé':'libelle02'}, inplace=True)
        indicateurs.drop(columns=['Nom de la variable'], inplace=True)

    if len(varname.split('_')) == 3 :
        df['code03'] = df.indicateur.apply(lambda x: x.split('_')[2])
        df['modalite03'] = df.modalites.apply(lambda x: x.split('_')[2])
        indicateurs = pd.merge(indicateurs, dico_variables [['Nom de la variable', 'Libellé']], left_on='code03', right_on='Nom de la variable', how='inner')
        indicateurs.rename(columns={'Libellé':'libelle03'}, inplace=True)
        indicateurs.drop(columns=['Nom de la variable'], inplace=True)
        
    return indicateurs



#########################################################################
#### Version 1 de la base en long ('Ensemble', 'Etranger', 'Français par acquisition', 'Français par acquisition', 'SecondeGeneration', 'Immigrés' en croisés avec la variable NAT2)
#########################################################################

# modalites_variables = database.columns[3:].tolist()
# modalites_variables

def fusion_EPCI_NAT2() : 
    '''
    Pour chaque unité, et chaque modalité de la variable passée en paramètre, on calcule les comptes par nationalité et par sous-ensembles :
    'Ensemble', 'Etranger', 'Français par acquisition', 'Français de naissance', 'SecondeGeneration', 'Immigrés'
    On créé la table nat_epci_long
    '''
    
    df = process_NAT_EPCI_first()
    df2 = process_INAT_EPCI()
    df3 = process_GEN2_NAT_EPCI()
    df4 = process_IMMI_NAT_EPCI()

    fusion = pd.merge(df, df2, on=['unit', 'NAT2'], how='left') 
    fusion = pd.merge(fusion, df3, on=['unit', 'NAT2'], how='left') 
    fusion = pd.merge(fusion, df4, on=['unit', 'NAT2'], how='left') 

    fusion.fillna(0, inplace=True)
    print(f"Taille de fusion is {fusion.shape}")
    #Typer toutes les colonnes comme des entiers
    map_columnsTypes = {'Etranger': 'int', 'Français par acquisition': 'int', 'Français de naissance': 'int', 'SecondeGeneration' : 'int', 'PremiereGeneration' : 'int', 'Immigrés' : 'int', 'Non immigrés' : 'int'}
    for f in FRENCH_DOMTOM:
        map_columnsTypes[f] = 'int'
    #print(map_columnsTypes)
    fusion = fusion.astype(dtype = map_columnsTypes)

    # Souvent les effectifs de seconde génération sont anonymisés et pas première génération. Ce calcul permet d'avoir un chiffre plus précis
    # fusion.SecondeGeneration = fusion.Ensemble - fusion.PremiereGeneration
    # Contre les lois de l'anonymat (effectifs à moins de 16 possible)

    #Réordonner les colonnes pour mettre toutes les variantes de français à la fin (quitte à les laisser tomber...)
    basic_colonnes = ['unit', 'NAT2', 'anneeRp', 'indicateur', 'indicateurCode', 'indicateurMode', 'Ensemble', 'Etranger', 'Français par acquisition', 'Français de naissance', 'SecondeGeneration', 'Immigrés']
    #fusion = fusion[basic_colonnes + [col for col in fusion.columns if col not in basic_colonnes]]
    #save_to_database(fusion[basic_colonnes + [col for col in fusion.columns if col not in basic_colonnes]], 'fusion_epci', 'imhana')
    save_to_database(fusion[basic_colonnes], 'nat_epci_long', 'imhana')

def fusion_EPCI_niveau1(variable = 'SEXE') : 
    '''
    Pour chaque unité, et chaque modalité de la variable passée en paramètre, on calcule les comptes par nationalité et par sous-ensembles :
    'Ensemble', 'Etranger', 'Français par acquisition', 'Français de naissance', 'SecondeGeneration', 'Immigrés'
    C'est ajouté à la suite dans la table nat_epci_long
    '''
        
    df = process_niveau1_EPCI_first(variable) #(477360, 8) pour les comptes de 'Ensemble'
    df2 = process_niveau1_EPCI_correspondances('INAT', variable) #(157960, 18) pour les comptes de  'Etranger', 'Français par acquisition', 'Français de naissance'
    df3 = process_niveau1_EPCI_correspondances('GEN2',variable) #(157960, 5) pour les comptes de  'SecondeGeneration', 'Immigrés'

    fusion = pd.merge(df, df2, on=['unit', 'NAT2', 'indicateurMode'], how='left') 
    fusion = pd.merge(fusion, df3, on=['unit', 'NAT2', 'indicateurMode'], how='left') 

    fusion.fillna(0, inplace=True)
    print(f"Taille de fusion pour niveau 1 {variable} is {fusion.shape}")
    #Typer toutes les colonnes comme des entiers
    map_columnsTypes = {'Etranger': 'int', 'Français par acquisition': 'int', 'Français de naissance': 'int', 'SecondeGeneration' : 'int', 'Immigrés' : 'int'}
    for f in FRENCH_DOMTOM:
        map_columnsTypes[f] = 'int'
    #print(map_columnsTypes)
    fusion = fusion.astype(dtype = map_columnsTypes)

    # Souvent les effectifs de seconde génération sont anonymisés et pas première génération. Ce calcul permet d'avoir un chiffre plus précis
    # fusion.SecondeGeneration = fusion.Ensemble - fusion.PremiereGeneration
    # Contre les lois de l'anonymat (effectifs à moins de 16 possible)

    #Réordonner les colonnes pour mettre toutes les variantes de français à la fin (quitte à les laisser tomber...)
    basic_colonnes = ['unit', 'NAT2', 'anneeRp', 'indicateur', 'indicateurCode', 'indicateurMode', 'Ensemble', 'Etranger', 'Français par acquisition', 'Français de naissance', 'SecondeGeneration', 'Immigrés']
    #fusion = fusion[basic_colonnes + [col for col in fusion.columns if col not in basic_colonnes]]
    #append_to_database(fusion[basic_colonnes + [col for col in fusion.columns if col not in basic_colonnes]], 'fusion_epci', 'imhana')
    
    append_to_database(fusion[basic_colonnes], 'nat_epci_long', 'imhana')
    
    
def summary_NAT_EPCI (variable=None):
    #datapath = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\EPCI\\"
    #epcisuffix = "_EPCI_2026.01.22.csv"
    #nationalites_speciales = ['Tous', 'etrangers', 'francaisParAcquisition', 'immigres']
    #EPCI_fictives = ['HORS__GFP', 'RESTANT__GFP', 'fictive_200027399', 'fictive_242320034', 'fictive_242320059', 'fictive_244701389']

    nat_epci_file = datapath+"NAT"+epcisuffix

    if variable :
        nat_epci_file = datapath+variable+"_NAT"+epcisuffix
        demographieCSVtype[variable] = 'string'
        
    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)

    if variable is None:     
        demo01 = demographie_etrangers.query("NAT2 in @nationalites_speciales  and not(unit in @doublons_CC_EPCI) and not(unit in @doublons_CA_CU_EPCI) ").pivot(index=['unit', 'NOM'], columns='NAT2', values='total_s')
        demo02 = demographie_etrangers.query("NAT2 in @nationalites_speciales and (unit in @doublons_CC_EPCI) ").pivot_table(index=['unit', 'NOM'], columns='NAT2', values='total_s',aggfunc="max") 
        demo03 = demographie_etrangers.query("NAT2 in @nationalites_speciales and (unit in @doublons_CA_CU_EPCI) ").pivot_table(index=['unit', 'NOM'], columns='NAT2', values='total_s',aggfunc="sum") 
    else: 
        demo01 = demographie_etrangers.query("NAT2 in @nationalites_speciales  and not(unit in @doublons_CC_EPCI) and not(unit in @doublons_CA_CU_EPCI) ").pivot(index=['unit', 'NOM', variable], columns='NAT2', values='total_s')
        demo02 = demographie_etrangers.query("NAT2 in @nationalites_speciales and (unit in @doublons_CC_EPCI) ").pivot_table(index=['unit', 'NOM', variable], columns='NAT2', values='total_s',aggfunc="max") 
        demo03 = demographie_etrangers.query("NAT2 in @nationalites_speciales and (unit in @doublons_CA_CU_EPCI) ").pivot_table(index=['unit', 'NOM', variable], columns='NAT2', values='total_s',aggfunc="sum") 

    frames = [demo01, demo02, demo03]

    result = pd.concat(frames)
    result.rename_axis(columns=None, inplace=True)
    result.reset_index(inplace=True)
    result.fillna(0, inplace=True)

    if variable :
        result.rename(columns={variable : 'indicateurMode'}, inplace=True)
        result = result.astype(dtype = {'indicateurMode': 'string'})  
    else : 
        result['indicateurMode']  = ''
        
    print(result.columns) #['unit', 'NOM', 'indicateurMode', 'Tous', 'etrangers', 'francaisParAcquisition','immigres']
    print(result.shape) #(1224, 6) ou (1224, 7) si variable

    result = result.astype(dtype = {'unit': 'string', 'NOM': 'string', 'Tous': 'int', 'etrangers': 'int', 'francaisParAcquisition': 'int', 'immigres': 'int'})   
    result['anneeRp']  = 2021
    result['indicateur']  = 'NAT2'+CODESEP
    result['indicateurCode']  =  'NAT2' if variable is None else variable
    if variable :
        result['indicateur'] = result['indicateurCode']+CODESEP+result['indicateurMode']
    # Réordonner les colonnes et garder la colonne NOM
    # (ainsi, il y a au moins une table non géographique qui garde le lien entre code unité et nom de l'interco. pour l'année du recensement)    
    result = result[['unit', 'NOM', 'anneeRp', 'indicateur', 'indicateurCode', 'indicateurMode', 'Tous', 'etrangers', 'francaisParAcquisition','immigres']]
    
    # Présuppose qu'on a d'abord appelé la fonction sans variable...
    if variable :
        append_to_database(result, 'resumes_nat_epci_long', 'imhana')
    else : 
        save_to_database(result, 'resumes_nat_epci_long', 'imhana')

    return  result

def process_NAT_COM_first():
    '''
    appelé par fusion_COM_NAT2 pour initialiser la table nat_com_long avec les totaux ('Ensemble') de nationalité par unité (les communes)
    Doit être appelé en premier car définit les colonnes anneeRp, indicateur, indicateurCode, indicateurMode
    La nationalité NAT2 est recodée : elle prend pour valeur NAT (nationalité actuelle) ou NATN si l'individu a été naturalisé français. 
    Ce qui fait que les comptes Ensemble définissent la somme des personnes d'origine d'une nationalité, qu'ils soient encore de cette nationalité, ou bien aient été naturalisés français. 
    '''
    #datapath = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\commune\\"
    #epcisuffix = "_COM_2026.01.22.csv"

    nat_epci_file = datapath+"NAT"+epcisuffix
    #"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\2026.01.22\commune\NAT_COM_2026.01.22.csv"

    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)

    demo01 = demographie_etrangers.query("NAT2 not in @nationalites_speciales and not(unit in @doublons_COM) ").pivot(index=['unit', 'NOM'], columns='NAT2', values='total_s')
    demo02 = demographie_etrangers.query("NAT2 not in @nationalites_speciales and (unit in @doublons_COM) ").pivot_table(index=['unit', 'NOM'], columns='NAT2', values='total_s',aggfunc="max") 

    frames = [demo01, demo02]

    result = pd.concat(frames) #2132 rows × 4 columns
    result.fillna(0, inplace=True)
    result.rename_axis(columns=None, inplace=True)
    result.reset_index(inplace=True)

    print(result.columns)
    print(result.shape) #(2132, 197)

    database = result.melt(id_vars=['unit', 'NOM'], value_vars=liste_natio, var_name='NAT2', value_name='Ensemble')
    database = database.astype(dtype = {'unit': 'string', 'NOM': 'string', 'NAT2': 'string', 'Ensemble': 'int'})

    print(database.shape) #(416130, 4)
    print(database.columns) #['unit', 'NOM', 'NAT2', 'Ensemble']

    database['anneeRp']  = 2021
    database['indicateur']  = 'NAT2'+CODESEP
    database['indicateurCode']  = 'NAT2'
    database['indicateurMode']  = ''
    #Retirer la colonne NOM
    return database[['unit', 'anneeRp', 'indicateur', 'indicateurCode', 'indicateurMode', 'NAT2', 'Ensemble']]

def process_COM_correspondances(croix= 'GEN2', variable='SEXE') :
    # variable = 'SEXE'
    # croix = 'GEN2'
    correspondances = {'INAT':'INAT_BIS', 'GEN2' : 'GENERATION2', 'IMMI' : 'IMMI'}

    nat_epci_file = datapath+croix+"_NAT"+epcisuffix
    if (variable) : 
        nat_epci_file = datapath+variable+"_"+croix+"_NAT"+epcisuffix
    #nat_epci_file = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\EPCI\NAT_EPCI_2026.01.22.csv"

    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)

    if (variable) : 
        demo01 = demographie_etrangers.query("NAT2 not in @nationalites_speciales  and not(unit in @doublons_COM)  ").pivot(index=['unit', 'NOM', 'NAT2', variable], columns=correspondances[croix], values='total_s')
        demo02 = demographie_etrangers.query("NAT2 not in @nationalites_speciales and (unit in @doublons_COM) ").pivot_table(index=['unit', 'NOM', 'NAT2', variable], columns=correspondances[croix], values='total_s',aggfunc="max") 
    else : 
        demo01 = demographie_etrangers.query("NAT2 not in @nationalites_speciales  and not(unit in @doublons_COM)  ").pivot(index=['unit', 'NOM', 'NAT2'], columns=correspondances[croix], values='total_s')
        demo02 = demographie_etrangers.query("NAT2 not in @nationalites_speciales and (unit in @doublons_COM) ").pivot_table(index=['unit', 'NOM', 'NAT2'], columns=correspondances[croix], values='total_s',aggfunc="max") 
        
    frames = [demo01, demo02]
    result = pd.concat(frames)
    result.fillna(0, inplace=True)
    result.rename_axis(columns=None, inplace=True)
    result.reset_index(inplace=True)

    if variable : 
        # renommer la colonne variable par indicateurMode 
        result.rename(columns={variable : 'indicateurMode'}, inplace=True)
        if (croix == 'INAT') :
            #Rajouter une colonne Français de naissance
            result['Français de naissance'] = result[FRENCH_DOMTOM].sum(axis=1)
            #Réordonner les colonnes, et retirer NOM
            result = result[['unit', 'NAT2', 'indicateurMode', 'Etranger', 'Français par acquisition', 'Français de naissance'] + [col for col in result.columns if col not in ['unit', 'NOM', 'NAT2', 'indicateurMode', 'Etranger', 'Français par acquisition', 'Français de naissance']]]
        if (croix ==  'GEN2') : 
            #Attention, petite cuisine malhonnète ici : Immigrés vaut normalement 'PremiereGeneration'
            result.rename(columns={True:'SecondeGeneration', False:'Immigrés'}, inplace=True)
            # retirer NOM des colonnes
            result = result[['unit', 'NAT2', 'indicateurMode', 'SecondeGeneration', 'Immigrés'] ]#PremiereGeneration 
            # bricolage car IMMI n'est pas croisé avec les variables de niveau 1 et Immigrés = PremiereGeneration
        if (croix ==  'IMMI') : 
            # retirer NOM des colonnes
            result = result[['unit', 'NAT2', 'indicateurMode', 'Immigrés', 'Non immigrés'] ]
    else : 
        
        if (croix == 'INAT') :
            #Rajouter une colonne Français de naissance
            result['Français de naissance'] = result[FRENCH_DOMTOM].sum(axis=1)
            #Réordonner les colonnes, et retirer NOM
            result = result[['unit', 'NAT2',  'Etranger', 'Français par acquisition', 'Français de naissance'] + [col for col in result.columns if col not in ['unit', 'NOM', 'NAT2', 'Etranger', 'Français par acquisition', 'Français de naissance']]]
        if (croix ==  'GEN2') : 
            #Attention, petite cuisine malhonnète ici : Immigrés vaut normalement 'PremiereGeneration'
            result.rename(columns={True:'SecondeGeneration', False:'Immigrés'}, inplace=True)
            # retirer NOM des colonnes
            result = result[['unit', 'NAT2',  'SecondeGeneration', 'Immigrés'] ]#PremiereGeneration 
            # bricolage car IMMI n'est pas croisé avec les variables de niveau 1 et Immigrés = PremiereGeneration
        if (croix ==  'IMMI') : 
            # retirer NOM des colonnes
            result = result[['unit', 'NAT2', 'Immigrés', 'Non immigrés'] ]
    print(result.columns)
    print(result.shape) #(157960, 18)

    return result


def fusion_COM_NAT2() : 
    '''
    Pour chaque unité, et chaque modalité de la variable passée en paramètre, on calcule les comptes par nationalité et par sous-ensembles :
    'Ensemble', 'Etranger', 'Français par acquisition', 'Français de naissance', 'SecondeGeneration', 'Immigrés'
    On créé la table nat_com_long
    '''
    
    df = process_NAT_COM_first()
    df2 = process_COM_correspondances('INAT',None)
    df3 = process_COM_correspondances('GEN2',None)

    fusion = pd.merge(df, df2, on=['unit', 'NAT2'], how='left') 
    fusion = pd.merge(fusion, df3, on=['unit', 'NAT2'], how='left') 

    fusion.fillna(0, inplace=True)
    print(f"Taille de fusion is {fusion.shape}")
    #Typer toutes les colonnes comme des entiers
    map_columnsTypes = {'Etranger': 'int', 'Français par acquisition': 'int', 'Français de naissance': 'int', 'SecondeGeneration' : 'int', 'Immigrés' : 'int'}
    for f in FRENCH_DOMTOM:
        map_columnsTypes[f] = 'int'
    #print(map_columnsTypes)
    fusion = fusion.astype(dtype = map_columnsTypes)

    # Souvent les effectifs de seconde génération sont anonymisés et pas première génération. Ce calcul permet d'avoir un chiffre plus précis
    # fusion.SecondeGeneration = fusion.Ensemble - fusion.PremiereGeneration
    # Contre les lois de l'anonymat (effectifs à moins de 16 possible)

    #Réordonner les colonnes pour mettre toutes les variantes de français à la fin (quitte à les laisser tomber...)
    basic_colonnes = ['unit', 'NAT2', 'anneeRp', 'indicateur', 'indicateurCode', 'indicateurMode', 'Ensemble', 'Etranger', 'Français par acquisition', 'Français de naissance', 'SecondeGeneration', 'Immigrés']
    #fusion = fusion[basic_colonnes + [col for col in fusion.columns if col not in basic_colonnes]]
    #save_to_database(fusion[basic_colonnes + [col for col in fusion.columns if col not in basic_colonnes]], 'fusion_epci', 'imhana')
    save_to_database(fusion[basic_colonnes], 'nat_com_long', 'imhana')
    
    
def summary_NAT_COM (variable=None):
    #datapath = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\EPCI\\"
    #epcisuffix = "_EPCI_2026.01.22.csv"
    #nationalites_speciales = ['Tous', 'etrangers', 'francaisParAcquisition', 'immigres']
    #EPCI_fictives = ['HORS__GFP', 'RESTANT__GFP', 'fictive_200027399', 'fictive_242320034', 'fictive_242320059', 'fictive_244701389']
    nat_epci_file = datapath+"NAT"+epcisuffix

    if variable :
        nat_epci_file = datapath+variable+"_NAT"+epcisuffix

    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)

    if variable is None:     
        demo01 = demographie_etrangers.query("NAT2 in @nationalites_speciales and not(unit in @doublons_COM) ").pivot(index=['unit', 'NOM'], columns='NAT2', values='total_s')
        demo02 = demographie_etrangers.query("NAT2 in @nationalites_speciales and (unit in @doublons_COM) ").pivot_table(index=['unit', 'NOM'], columns='NAT2', values='total_s',aggfunc="max") 
    else: 
        demo01 = demographie_etrangers.query("NAT2 in @nationalites_speciales  and not(unit in @doublons_COM) ").pivot(index=['unit', 'NOM', variable], columns='NAT2', values='total_s')
        demo02 = demographie_etrangers.query("NAT2 in @nationalites_speciales and (unit in @doublons_COM) ").pivot_table(index=['unit', 'NOM', variable], columns='NAT2', values='total_s',aggfunc="max") 

    frames = [demo01, demo02]

    result = pd.concat(frames)
    result.rename_axis(columns=None, inplace=True)
    result.reset_index(inplace=True)
    result.fillna(0, inplace=True)

    if variable :
        result.rename(columns={variable : 'indicateurMode'}, inplace=True)
        result = result.astype(dtype = {'indicateurMode': 'string'})  
    else : 
        result['indicateurMode']  = ''
        
    print(result.columns) #['unit', 'NOM', 'indicateurMode', 'Tous', 'etrangers', 'francaisParAcquisition','immigres']
    print(result.shape) #(1224, 6) ou (1224, 7) si variable

    result = result.astype(dtype = {'unit': 'string', 'NOM': 'string', 'Tous': 'int', 'etrangers': 'int', 'francaisParAcquisition': 'int', 'immigres': 'int'})   
    result['anneeRp']  = 2021
    result['indicateur']  = 'NAT2'+CODESEP
    result['indicateurCode']  =  'NAT2' if variable is None else variable
    if variable :
        result['indicateur'] = result['indicateurCode']+CODESEP+result['indicateurMode']
    # Réordonner les colonnes et garder la colonne NOM
    # (ainsi, il y a au moins une table non géographique qui garde le lien entre code unité et nom de la commune. pour l'année du recensement)    
    result = result[['unit', 'NOM', 'anneeRp', 'indicateur', 'indicateurCode', 'indicateurMode', 'Tous', 'etrangers', 'francaisParAcquisition','immigres']]
    
    # Présuppose qu'on a d'abord appelé la fonction sans variable...
    if variable :
        append_to_database(result, 'resumes_nat_com_long', 'imhana')
    else : 
        save_to_database(result, 'resumes_nat_com_long', 'imhana')

    return  result

#########################################################################
#### Version 1 de la base en long (les nationalités dans la colonne NAT2, les indicateurs dans la colonne indicateur), 
# décomposés en ligne suivant Ensemble, Etranger, Français par acquisition, Français de naissance, SecondeGénération, Immigrés
#########################################################################

"""
ici = 'faire la table nat_epci_long'
print(ici)

#fusion_EPCI_NAT2() #initialise la table nat_epci_long
summary_NAT_EPCI()
"""

ici = 'mettre à jour la table nat_epci_long'
print(ici)

#colonnes = [ 'SEXE']
colonnes =  [ 'DIPLR', 'POSP', 'CATPR', 'IRANR',  'LTEXC', 'MODTRANS', 'AGER','STAT', 'STATCONJ', 'TACT', 'IMMI',  'ARRIVR']
# mises de côté car beaucoup de modalités : 'LRANE', 'COMMUNE_RESIDANTER', 'NAT3', 'DEPT_NAIS', 'DENSITE7_RESID', 'DENSITE7_RESIDANTER', 'DENSITE7_TRAV', 'LTEXD' , 'LNAIE',
for var in colonnes:
    #Ajoute les lignes pour l'indicateur dans nat_epci_long
    fusion_EPCI_niveau1(variable = var)
    #Ajoute les lignes pour l'indicateur dans resumes_nat_epci_long
    summary_NAT_EPCI(variable = var)
    

ici = 'ajouter les logements et leurs résumés'    
print(ici)

if platform.system() == 'Windows':
    datapath = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\EPCI\logement\\"
else : 
    datapath = '/home/cperreau/imhana/export_CASD_ergonomiques/2026.01.22/EPCI/logement/'
#datapath  = os.path.join(datapath, 'logement')
colonnes = ['ACHLR', 'HLML',  'INPER', 'INPOM', 'INPSM', 'NBPIR', 'NPER',  'STOCD', 'SURF', 'TYPL', 'VOIT' ]
# Rappel : Dina a recodé les variables INPER, INPOM, INPSM et NPER en 5 modalités : 0 pers, 1 pers, 2 pers, 3 pers, 4 pers, 5 pers ou plus.
for var in colonnes:
    #Ajoute les lignes pour l'indicateur dans nat_epci_long
    fusion_EPCI_niveau1(variable = var)
    summary_NAT_EPCI(variable = var)


### Les COMMUNES

""" 
if platform.system() == 'Windows':
    datapath = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\commune\\"
else : 
    datapath = '/home/cperreau/imhana/export_CASD_ergonomiques/2026.01.22/commune/'
    
#datapath = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\commune\\"
epcisuffix = "_COM_2026.01.22.csv"

ici = 'faire la table nat_com_long'
print(ici)
fusion_COM_NAT2() #initialise la table nat_com_long de taille 416130 lignes

ici = 'résumés des individus communes'
print(ici)
summary_NAT_COM()

"""


## DEPRECATED
#df = process_niveau1_EPCI(variable = 'INAT')
#save_to_database(df, 'nat_epci_INAT', 'imhana')

#df = process_niveau1_EPCI(variable = 'DIPLR')
#save_to_database(df, f"nat_epci_{'DIPLR'.lower()}", 'imhana')
#add_columns_to_nat_epci(df, 'DIPLR'.lower())

# colonnes =  [ 'POSP', 'CATPR', 'IRANR',  'LTEXC', 'MODTRANS', 'AGER','STAT', 'STATCONJ', 'TACT', 'IMMI', 'LNAIE', 'ARRIVR']
# for var in colonnes:
#     df = process_niveau1_EPCI(variable = var)
#     save_to_database(df, f"nat_epci_{var.lower()}", 'imhana')
#     add_columns_to_nat_epci(df, var.lower())




#########################################################################
#### Version 2 de la base en large (les nationalités en colonnes) mais uniquement Ensemble comme catégorie. 
#########################################################################

## Premiers dataframes (à faire, obligaoire !)

""" df = process_NAT_EPCI_wide()
save_to_database(df, 'nat_epci_wide', 'imhana')

indicateurs = make_dico_variables('NAT2', df.indicateurMode.unique().tolist())
save_to_database(indicateurs, 'indicateurs', 'imhana') """

# A faire ensuite pour garder les indicateurs croisés
"""
alter table imhana.indicateurs add column code02 text;
alter table imhana.indicateurs add column modalite02 text;
alter table imhana.indicateurs add column libelle02 text;

alter table imhana.indicateurs add column code03 text;
alter table imhana.indicateurs add column modalite03 text;
alter table imhana.indicateurs add column libelle03 text;
"""

## Suivants dataframes
"""
df = process_niveau1_EPCI_wide('SEXE')
append_to_database(df, 'nat_epci_wide', 'imhana')
indicateurs = make_dico_variables('SEXE', df.indicateurMode.unique().tolist())
append_to_database(indicateurs, 'indicateurs', 'imhana')


colonnes =  [ 'DIPLR', 'POSP', 'CATPR', 'IRANR',  'LTEXC', 'MODTRANS', 'AGER','STAT', 'STATCONJ', 'TACT', 'IMMI', 'LNAIE', 'ARRIVR']
# mises de côté car beaucoup de modalités : 'LRANE', 'COMMUNE_RESIDANTER', 'NAT3', 'DEPT_NAIS', 'DENSITE7_RESID', 'DENSITE7_RESIDANTER', 'DENSITE7_TRAV', 'LTEXD' , 'LNAIE',
"""

""" for var in colonnes:
    df = process_niveau1_EPCI_wide(var)
    append_to_database(df, 'nat_epci_wide', 'imhana')
    indicateurs = make_dico_variables(var, df.indicateurMode.unique().tolist())
    append_to_database(indicateurs, 'indicateurs', 'imhana') """


#c:\Travail\Enseignement\Cours_M2_python\2025\Projet_INSEE\Insee\prepare_database.py:113: DtypeWarning: Columns (0) have mixed types. Specify dtype option on import or set low_memory=False.

#niveau2 : AGER x [SEXE, POSP, DIPLR] et Sexe x [STAT, STATCONJ]
# Test pour croisement sur indicateurs
# indicateurs = make_dico_variables('SEXE_AGER', df.indicateurMode.unique().tolist())

# Logement - niveau 1 

# datapath = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\EPCI\logement\\"
# colonnes = ['ACHLR', 'HLML',  'INPER', 'INPOM', 'INPSM', 'NBPIR', 'NPER',  'STOCD', 'SURF', 'TYPL', 'VOIT' ]

# Rappel : Dina a recodé les variables INPER, INPOM, INPSM et NPER en 5 modalités : 0 pers, 1 pers, 2 pers, 3 pers, 4 pers, 5 pers ou plus.

#Fait
""" for var in colonnes:
    df = process_niveau1_EPCI_wide(var)
    append_to_database(df, 'nat_epci_wide', 'imhana')
    indicateurs = make_dico_variables(var, df.indicateurMode.unique().tolist(), 'RP_logements_principale')
    append_to_database(indicateurs, 'indicateurs', 'imhana') """
    
# niveau2 : AGER x [TYPL, STOCD] 

