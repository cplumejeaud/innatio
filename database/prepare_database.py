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

os.chdir('C:\Travail\Enseignement\Cours_M2_python\\2025\Projet_INSEE\Insee')

datapath = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\EPCI\\"
suffix = "_2026.01.22.csv"
epcisuffix = "_EPCI_2026.01.22.csv"
# - NAT_EPCI_2026.01.22.csv
# - INAT_NAT_EPCI_2026.01.22.csv
# - GEN2_NAT_EPCI_2026.01.22.csv
# - IMMI_NAT_EPCI_2026.01.22.csv

EPT_Paris = ['200057867','200057875','200057941','200057966','200057974','200057982','200057990','200058006','200058014','200058097','200058790']
EPCI_fictives = ['HORS__GFP', 'RESTANT__GFP', 'fictive_200027399', 'fictive_242320034', 'fictive_242320059', 'fictive_244701389']

nationalites_speciales = ['Tous', 'etrangers', 'francaisParAcquisition', 'immigres']
liste_natio = pd.read_csv('liste_nationalites.csv')['nationalites'].tolist()

doublons_CC_EPCI = pd.read_csv('doublons_CC_EPCI.csv', sep=';', encoding='utf-8', dtype={'doublons_CC_EPCI':str})['doublons_CC_EPCI'].tolist()
doublons_CA_CU_EPCI = pd.read_csv('doublons_CA_CU_EPCI.csv', sep=';', encoding='utf-8', dtype={'doublons_CA_CU_EPCI':str})['doublons_CA_CU_EPCI'].tolist()
print(doublons_CC_EPCI)
print(doublons_CA_CU_EPCI)

STRANGERS = ['Etranger', 'Français par acquisition']
FRENCH_DOMTOM = pd.read_csv('FRENCH_DOMTOM.csv', sep=';', encoding='utf-8', dtype={'FRENCH_DOMTOM':str})['FRENCH_DOMTOM'].tolist()

def process_NAT_EPCI():
    nat_epci_file = datapath+"NAT"+epcisuffix
    #nat_epci_file = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\EPCI\NAT_EPCI_2026.01.22.csv"

    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)
    
    demo01 = demographie_etrangers.query(" NAT2 not in @nationalites_speciales and not(unit in @EPCI_fictives) and not(unit in @doublons_CC_EPCI) and not(unit in @doublons_CA_CU_EPCI) ").pivot(index=['unit', 'NOM'], columns='NAT2', values='total_s')
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
    database['indicateur']  = 'NAT'
    database['indicateurCode']  = 'NAT2'
    database['indicateurMode']  = ''
    #Retirer la colonne NOM
    database = database[['unit', 'anneeRp', 'indicateur', 'indicateurCode', 'indicateurMode', 'NAT2', 'Ensemble']]
    
    return  database

def process_INAT_EPCI():
    nat_epci_file = datapath+"INAT_NAT"+epcisuffix
    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)
    
    demo01 = demographie_etrangers.query(" NAT2 not in @nationalites_speciales and not(unit in @EPCI_fictives) and not(unit in @doublons_CC_EPCI) and not(unit in @doublons_CA_CU_EPCI) ").pivot(index=['unit', 'NOM', 'NAT2'], columns='INAT_BIS', values='total_s')
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
    nat_epci_file = datapath+"GEN2_NAT"+epcisuffix
    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)

    demo01 = demographie_etrangers.query(" NAT2 not in @nationalites_speciales and not(unit in @EPCI_fictives) and not(unit in @doublons_CC_EPCI) and not(unit in @doublons_CA_CU_EPCI) ").pivot(index=['unit', 'NOM', 'NAT2'], columns='GENERATION2', values='total_s')
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
    nat_epci_file = datapath+"IMMI_NAT"+epcisuffix
    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)

    demo01 = demographie_etrangers.query(" NAT2 not in @nationalites_speciales and not(unit in @EPCI_fictives) and not(unit in @doublons_CC_EPCI) and not(unit in @doublons_CA_CU_EPCI) ").pivot(index=['unit', 'NOM', 'NAT2'], columns='IMMI', values='total_s')
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
    nat_epci_file = datapath+variable+"_NAT_EPCI"+suffix
    #nat_epci_file = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\EPCI\NAT_EPCI_2026.01.22.csv"

    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)

    demo01 = demographie_etrangers.query("NAT2 not in @nationalites_speciales and not(unit in @EPCI_fictives) and not(unit in @doublons_CC_EPCI) and not(unit in @doublons_CA_CU_EPCI) ").pivot(index=['unit', 'NOM', variable], columns='NAT2', values='total_s')
    demo02 = demographie_etrangers.query("NAT2 not in @nationalites_speciales and (unit in @doublons_CC_EPCI) ").pivot_table(index=['unit', 'NOM', variable], columns='NAT2', values='total_s',aggfunc="max") 
    demo03 = demographie_etrangers.query("NAT2 not in @nationalites_speciales and (unit in @doublons_CA_CU_EPCI) ").pivot_table(index=['unit', 'NOM', variable], columns='NAT2', values='total_s',aggfunc="sum") 

    frames = [demo01, demo02, demo03]

    result = pd.concat(frames)
    result.fillna(0, inplace=True)
    result.rename_axis(columns=None, inplace=True)
    result.reset_index(inplace=True)

    #print(result.columns)
    #print(result.shape) #(2448, 198)

    database = result.melt(id_vars=['unit', 'NOM', variable], value_vars=liste_natio, var_name=['NAT2'], value_name='total_s')
    database = database.astype(dtype = {'unit': 'string', 'NOM': 'string', 'NAT2': 'string', 'total_s': 'int'})
    database.rename(columns={variable : 'indicateurMode', 'total_s':'Ensemble'}, inplace=True)
    database['anneeRp']  = 2021
    database['indicateur']  = variable
    database['indicateurCode']  = variable
    
    print(database.shape) #(477360, 5)
    print(database.columns) #['unit', 'NOM', 'indicateurMode', 'NAT2', 'Ensemble']
    #print(database.dtypes)

    return database[['unit', 'NAT2', 'anneeRp', 'indicateur', 'indicateurCode', 'indicateurMode',  'Ensemble' ]]
    
def process_niveau1_EPCI_correspondances(croix= 'GEN2', variable='SEXE') :
    # variable = 'SEXE'
    # croix = 'GEN2'
    correspondances = {'INAT':'INAT_BIS', 'GEN2' : 'GENERATION2', 'IMMI' : 'IMMI'}

    nat_epci_file = datapath+variable+"_"+croix+"_NAT_EPCI"+suffix
    #nat_epci_file = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\EPCI\NAT_EPCI_2026.01.22.csv"

    demographie_etrangers = pd.read_csv(nat_epci_file, sep=';', encoding='latin1')
    demographie_etrangers['unit'] = demographie_etrangers['unit'].astype(str)

    #unit == '248500191' and  
    demo01 = demographie_etrangers.query("NAT2 not in @nationalites_speciales and not(unit in @EPCI_fictives) and not(unit in @doublons_CC_EPCI) and not(unit in @doublons_CA_CU_EPCI) ").pivot(index=['unit', 'NOM', 'NAT2', variable], columns=correspondances[croix], values='total_s')
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
    database_wide['indicateur'] =  'NAT' #servira de PK avec unit, et anneeRp et permettra de faire le lien avec les autres indicateurs
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
    database_wide['indicateur'] =  variable #servira de PK avec unit, et anneeRp et permettra de faire le lien avec les autres indicateurs
    database_wide['indicateurCode'] =  variable 
    database_wide['indicateurMode'] =  result[[variable]]
    database_wide['categorie'] = 'Ensemble' #	Etrangers, Français par acquisition,  SecondeGénération, Immigrés	

    result['indicateurMode']=  result[[variable]]
    database_wide = pd.merge(database_wide, result, on=['unit', 'indicateurMode'], how='left') 
    database_wide.drop(columns=['NOM', variable], inplace=True)
    print(database_wide.shape)
    #(1224, 206) avec les 4 colonnes de unit, anneeRp, indicateur, indicateurCode, indicateurMode, categorie, et les 197 colonnes de nationalités, 'Tous' puis 'etrangers', 'francaisParAcquisition', 'immigres')

    return  database_wide


def process_niveau1_EPCI(variable = 'SEXE'):
    nat_epci_file = datapath+variable+"_NAT_EPCI"+suffix
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
    database = result.melt(id_vars=['unit', 'NOM', variable], value_vars=liste_natio+nationalites_speciales, var_name=['NAT2'], value_name='total_s')
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


def save_to_database(df, table_name, schema_name='imhana2021'):

    engine = create_engine('postgresql://postgres:postgres@localhost:5432/inseedb', connect_args={'options': '-csearch_path={}'.format('imhana2021,public')})
    ORM_conn=engine.connect()
    df.to_sql(table_name, con=ORM_conn , schema=schema_name, if_exists='replace', index=False)
    ORM_conn.commit()
    ORM_conn.close() 
        
    # engine = create_engine('postgresql://postgres:postgres@localhost:5432/inseedb')
    # df.to_sql(table_name, con=engine , schema=schema_name, if_exists='replace', index=False)
    # engine.dispose()

def append_to_database(df, table_name, schema_name='imhana2021'):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/inseedb', connect_args={'options': '-csearch_path={}'.format('imhana2021,public')})
    df.to_sql(table_name, con=engine , schema=schema_name, if_exists='append', index=False)
    engine.dispose()

    



def add_columns_to_nat_epci(df, variable = 'sexe'):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/inseedb', connect_args={'options': '-csearch_path={}'.format('imhana2021,public')})
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
    df = process_NAT_EPCI()
    #save_to_database(df, 'nat_epci', 'imhana2021')

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
    #save_to_database(fusion[basic_colonnes + [col for col in fusion.columns if col not in basic_colonnes]], 'fusion_epci', 'imhana2021')
    save_to_database(fusion[basic_colonnes], 'basicfusion_epci', 'imhana2021')

def fusion_EPCI_niveau1(variable = 'SEXE') : 
    
    df = process_niveau1_EPCI_first(variable) #(477360, 8)
    #save_to_database(df, 'nat_epci', 'imhana2021')
    df2 = process_niveau1_EPCI_correspondances('INAT', variable) #(157960, 18)
    df3 = process_niveau1_EPCI_correspondances('GEN2',variable) #(157960, 5)

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
    #append_to_database(fusion[basic_colonnes + [col for col in fusion.columns if col not in basic_colonnes]], 'fusion_epci', 'imhana2021')
    append_to_database(fusion[basic_colonnes], 'basicfusion_epci', 'imhana2021')
    
    


"""
fusion_EPCI_NAT2() #initialise la table basicfusion_epci
colonnes =  [ 'SEXE', 'DIPLR', 'POSP', 'CATPR', 'IRANR',  'LTEXC', 'MODTRANS', 'AGER','STAT', 'STATCONJ', 'TACT', 'IMMI',  'ARRIVR']
# mises de côté car beaucoup de modalités : 'LRANE', 'COMMUNE_RESIDANTER', 'NAT3', 'DEPT_NAIS', 'DENSITE7_RESID', 'DENSITE7_RESIDANTER', 'DENSITE7_TRAV', 'LTEXD' , 'LNAIE',
for var in colonnes:
    #Ajoute les lignes pour l'indicateur dans basicfusion_epci
    df = fusion_EPCI_niveau1(variable = var)
"""

ici = 'logements'    

datapath = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\EPCI\logement\\"
colonnes = ['ACHLR', 'HLML',  'INPER', 'INPOM', 'INPSM', 'NBPIR', 'NPER',  'STOCD', 'SURF', 'TYPL', 'VOIT' ]
# Rappel : Dina a recodé les variables INPER, INPOM, INPSM et NPER en 5 modalités : 0 pers, 1 pers, 2 pers, 3 pers, 4 pers, 5 pers ou plus.
for var in colonnes:
    #Ajoute les lignes pour l'indicateur dans basicfusion_epci
    df = fusion_EPCI_niveau1(variable = var)

#df = process_niveau1_EPCI(variable = 'INAT')
#save_to_database(df, 'nat_epci_INAT', 'imhana2021')

#df = process_niveau1_EPCI(variable = 'DIPLR')
#save_to_database(df, f"nat_epci_{'DIPLR'.lower()}", 'imhana2021')
#add_columns_to_nat_epci(df, 'DIPLR'.lower())

# colonnes =  [ 'POSP', 'CATPR', 'IRANR',  'LTEXC', 'MODTRANS', 'AGER','STAT', 'STATCONJ', 'TACT', 'IMMI', 'LNAIE', 'ARRIVR']
# for var in colonnes:
#     df = process_niveau1_EPCI(variable = var)
#     save_to_database(df, f"nat_epci_{var.lower()}", 'imhana2021')
#     add_columns_to_nat_epci(df, var.lower())

#########################################################################
#### Version 2 de la base en large (les nationalités en colonnes)
#########################################################################

## Premiers dataframes (à faire, obligaoire !)

""" df = process_NAT_EPCI_wide()
save_to_database(df, 'nat_epci_wide', 'imhana2021')

indicateurs = make_dico_variables('NAT2', df.indicateurMode.unique().tolist())
save_to_database(indicateurs, 'indicateurs', 'imhana2021') """

# A faire ensuite pour garder les indicateurs croisés
"""
alter table imhana2021.indicateurs add column code02 text;
alter table imhana2021.indicateurs add column modalite02 text;
alter table imhana2021.indicateurs add column libelle02 text;

alter table imhana2021.indicateurs add column code03 text;
alter table imhana2021.indicateurs add column modalite03 text;
alter table imhana2021.indicateurs add column libelle03 text;
"""

## Suivants dataframes
"""
df = process_niveau1_EPCI_wide('SEXE')
append_to_database(df, 'nat_epci_wide', 'imhana2021')
indicateurs = make_dico_variables('SEXE', df.indicateurMode.unique().tolist())
append_to_database(indicateurs, 'indicateurs', 'imhana2021')


colonnes =  [ 'DIPLR', 'POSP', 'CATPR', 'IRANR',  'LTEXC', 'MODTRANS', 'AGER','STAT', 'STATCONJ', 'TACT', 'IMMI', 'LNAIE', 'ARRIVR']
# mises de côté car beaucoup de modalités : 'LRANE', 'COMMUNE_RESIDANTER', 'NAT3', 'DEPT_NAIS', 'DENSITE7_RESID', 'DENSITE7_RESIDANTER', 'DENSITE7_TRAV', 'LTEXD' , 'LNAIE',
"""

""" for var in colonnes:
    df = process_niveau1_EPCI_wide(var)
    append_to_database(df, 'nat_epci_wide', 'imhana2021')
    indicateurs = make_dico_variables(var, df.indicateurMode.unique().tolist())
    append_to_database(indicateurs, 'indicateurs', 'imhana2021') """


#c:\Travail\Enseignement\Cours_M2_python\2025\Projet_INSEE\Insee\prepare_database.py:113: DtypeWarning: Columns (0) have mixed types. Specify dtype option on import or set low_memory=False.

#niveau2 : AGER x [SEXE, POSP, DIPLR] et Sexe x [STAT, STATCONJ]
# Test pour croisement sur indicateurs
# indicateurs = make_dico_variables('SEXE_AGER', df.indicateurMode.unique().tolist())

#################################################################################################################################
# Logement - niveau 1
#################################################################################################################################


# datapath = r"C:\Travail\MIGRINTER\Labo\IMHANA\Méthodologie\Statistiques\export_CASD_ergonomiques\\2026.01.22\EPCI\logement\\"
# colonnes = ['ACHLR', 'HLML',  'INPER', 'INPOM', 'INPSM', 'NBPIR', 'NPER',  'STOCD', 'SURF', 'TYPL', 'VOIT' ]

# Rappel : Dina a recodé les variables INPER, INPOM, INPSM et NPER en 5 modalités : 0 pers, 1 pers, 2 pers, 3 pers, 4 pers, 5 pers ou plus.

#Fait
""" for var in colonnes:
    df = process_niveau1_EPCI_wide(var)
    append_to_database(df, 'nat_epci_wide', 'imhana2021')
    indicateurs = make_dico_variables(var, df.indicateurMode.unique().tolist(), 'RP_logements_principale')
    append_to_database(indicateurs, 'indicateurs', 'imhana2021') """
    
# niveau2 : AGER x [TYPL, STOCD] 

