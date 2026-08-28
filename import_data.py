"""
Import du fichier vente_billette.xlsx vers la base PostgreSQL 'ventes_billettes'.

Usage :
    python import_data.py chemin/vers/vente_billette.xlsx

Prérequis :
    - le conteneur postgres (docker compose up -d) doit tourner
    - pip install -r requirements.txt
"""

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://elfouladh:elfouladh_pwd@localhost:5432/ventes_billettes"


def load_excel(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    df.columns = [c.strip() for c in df.columns]

    # Nettoyage de base
    df["RAISON_SOCIALE"] = df["RAISON_SOCIALE"].str.strip()
    df["RUE"] = df["RUE"].str.strip()
    df["DESIGNATION"] = df["DESIGNATION"].str.strip()
    df["MODEPAIEMENT"] = df["MODEPAIEMENT"].fillna("NON PRECISE")

    # TEL / FAX : numériques avec NaN -> texte propre, NaN -> None
    for col in ["TEL", "FAX"]:
        df[col] = df[col].apply(
            lambda v: str(int(v)) if pd.notna(v) else None
        )

    df["DATE_V"] = pd.to_datetime(df["DATE_V"]).dt.date
    return df


def import_to_postgres(df: pd.DataFrame) -> None:
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        # 1. Dimension clients (dédoublonnée sur RAISON_SOCIALE)
        clients = (
            df[["RAISON_SOCIALE", "RUE", "TEL", "FAX"]]
            .drop_duplicates(subset=["RAISON_SOCIALE"])
        )
        for row in clients.itertuples(index=False):
            conn.execute(
                text("""
                    INSERT INTO dim_clients (raison_sociale, rue, tel, fax)
                    VALUES (:rs, :rue, :tel, :fax)
                    ON CONFLICT (raison_sociale) DO UPDATE
                    SET rue = EXCLUDED.rue, tel = EXCLUDED.tel, fax = EXCLUDED.fax
                """),
                {"rs": row.RAISON_SOCIALE, "rue": row.RUE, "tel": row.TEL, "fax": row.FAX},
            )

        # 2. Dimension articles (dédoublonnée sur CODE_ARTICLE)
        articles = (
            df[["CODE_ARTICLE", "DESIGNATION", "FAMILLE", "SECTION", "UNITE_MESURE"]]
            .drop_duplicates(subset=["CODE_ARTICLE"])
        )
        for row in articles.itertuples(index=False):
            conn.execute(
                text("""
                    INSERT INTO dim_articles (code_article, designation, famille, section, unite_mesure)
                    VALUES (:code, :desig, :fam, :sec, :unite)
                    ON CONFLICT (code_article) DO UPDATE
                    SET designation = EXCLUDED.designation,
                        famille = EXCLUDED.famille,
                        section = EXCLUDED.section,
                        unite_mesure = EXCLUDED.unite_mesure
                """),
                {
                    "code": int(row.CODE_ARTICLE),
                    "desig": row.DESIGNATION,
                    "fam": row.FAMILLE,
                    "sec": row.SECTION,
                    "unite": row.UNITE_MESURE,
                },
            )

        # 3. Table de faits ventes (jointure client_id via raison_sociale)
        client_ids = dict(
            conn.execute(text("SELECT raison_sociale, client_id FROM dim_clients")).fetchall()
        )

        rows = []
        for i, row in enumerate(df.itertuples(index=False), start=1):
            rows.append({
                "num": i,
                "client_id": client_ids[row.RAISON_SOCIALE],
                "code": int(row.CODE_ARTICLE),
                "date": row.DATE_V,
                "mode": row.MODEPAIEMENT,
                "qte": float(row.QUANTITE),
                "prix_cat": float(row.PRIX_CATALOGUE),
                "prix_ttc": float(row.PRIX_TTC),
                "prix": float(row.PRIX),
                "tva": int(row.TVA),
            })

        conn.execute(
            text("""
                INSERT INTO ventes
                    (numero_ligne, client_id, code_article, date_vente, mode_paiement,
                     quantite, prix_catalogue, prix_ttc, prix, tva)
                VALUES
                    (:num, :client_id, :code, :date, :mode, :qte, :prix_cat, :prix_ttc, :prix, :tva)
            """),
            rows,
        )

    print(f"Import terminé : {len(clients)} clients, {len(articles)} articles, {len(df)} ventes.")


if __name__ == "__main__":
    excel_path = sys.argv[1] if len(sys.argv) > 1 else "vente_billette.xlsx"
    if not Path(excel_path).exists():
        sys.exit(f"Fichier introuvable : {excel_path}")

    data = load_excel(excel_path)
    import_to_postgres(data)
