# carte_nationalites_par_epci.py

from flask import Blueprint, render_template, request, make_response
import pandas as pd
import geopandas as gpd
import json
# folium is imported lazily inside the route to avoid heavy imports at module import time
from sqlalchemy import create_engine
from sqlalchemy import text

import logging


bp = Blueprint('cartes', __name__, template_folder='templates')
logging.basicConfig(level=logging.INFO)

# Lazy cached GeoDataFrame
_geo_df = None

def get_geo_df():
    """Load geo DataFrame on first use and cache it. Returns GeoDataFrame (may be empty on error)."""
    global _geo_df
    if _geo_df is not None:
        return _geo_df

    try:
        #engine = create_engine("postgresql://postgres:postgres@localhost/savoie")
        #engine = create_engine("postgresql://postgres:postgres@localhost/inseedb")
        #engine = create_engine("postgresql://insee:insee2025@localhost:8010/inseedb")
        engine = create_engine("postgresql://insee:insee2025@localhost/inseedb")
        query = """
        SELECT 
            "EPCI",
            "nom_epci",
            "NAT_rec3" AS "Nationalite",
            "total_s",
            "part_etrg_epci",
            "geometry"
        FROM poisson_dina.nat_etrg_par_epci
        """
        ORM_conn=engine.connect()
        _geo_df = gpd.read_postgis(text(query), ORM_conn, geom_col="geometry")
        ORM_conn.close()
        print("Geo data loaded with", len(_geo_df), "records")
    except Exception as e:
        logging.warning('Could not load geo data: %s', e)
        _geo_df = gpd.GeoDataFrame()

    return _geo_df


# --- Page ou route principale ---
@bp.route("/nationalites_epci")
def index():
    gdf = get_geo_df()
    Nationalite = sorted(gdf["Nationalite"].unique()) if not gdf.empty else []
    return render_template("carte_nat_bis.html", Nationalite=Nationalite)


# --- Route pour générer la carte ---
@bp.route("/get_data_plot")
def get_data_plot():
    nat = request.args.get("Nationalite", "")  # récupère la nationalite choisie
    
    # Filtrer le GeoDataFrame pour la nationalité sélectionnée
    gdf = get_geo_df()
    geo_nationalite = gdf[gdf["Nationalite"] == nat] if not gdf.empty else gpd.GeoDataFrame()
    
    if geo_nationalite.empty:
        return make_response(
            json.dumps({"error": "Pas de données pour cette nationalité"}),
            200,
            {"Content-Type": "application/json"}
        )
    # Import folium lazily to avoid importing heavy network libs at app startup
    try:
        import folium
    except Exception as e:
        logging.warning('Could not import folium: %s', e)
        return make_response(
            json.dumps({"error": "Server missing folium module"}),
            500,
            {"Content-Type": "application/json"}
        )

    # Centrer la carte sur la région (coordonnées approximatives)
    # Création de la carte centrée sur la France
    m = folium.Map(
        location=[46.6, 2.5], 
        zoom_start=6,
        tiles="cartodbpositron"
    )
     # Calque choroplèthe pour la nationalité choisie
    folium.Choropleth(
        geo_data=geo_nationalite,
        name=f"Part {nat}",
        data=geo_nationalite,
        columns=["EPCI", "part_etrg_epci"],
        key_on="feature.properties.EPCI",
        fill_color="YlOrRd",
        fill_opacity=0.7,
        weight=1,
        color='black',
        nan_fill_color = 'grey',
        legend_name=f"Part de {nat} (%)"
    ).add_to(m)
    
    # Ajouter info-bulles
    folium.GeoJson(
        geo_nationalite,
        name="Infos",
        tooltip=folium.features.GeoJsonTooltip(
            fields=["nom_epci", "part_etrg_epci", 'total_s'],
            aliases=["EPCI :", "Part (%) :", "Fréquence absolue :"],
            localize=True
        )
    ).add_to(m)
    
    folium.LayerControl().add_to(m)
    
    # Retourner le HTML de la carte
    map_html = m._repr_html_()
    return make_response(
        json.dumps({"map_html": map_html}),
        200,
        {"Content-Type": "application/json"}
    )
    
if __name__ == "__main__":
    # Run standalone for debugging
    from flask import Flask
    app = Flask(__name__)
    app.register_blueprint(bp)
    # Preload geo data to fail early if desired
    try:
        get_geo_df()
    except Exception as e:
        print("Could not load geo data")
        print(e) #Could not load geo data: 'OptionEngine' object has no attribute 'execute'
        pass
    app.run(debug=True)
    