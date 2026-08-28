"""
DAG ETL - vente de billettes
Extract  : lit vente_billette.xlsx, nettoie les données -> fichier parquet intermédiaire
Transform: dédoublonne clients/articles, calcule un indicateur simple (CA par ligne)
Load     : insère clients, articles et ventes dans PostgreSQL (base ventes_billettes)
Report   : génère un PDF de synthèse et l'envoie par email au responsable (chaque semaine)
"""

from __future__ import annotations

import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pendulum
import pandas as pd
from fpdf import FPDF
from sqlalchemy import create_engine, text

from airflow.sdk import dag, task

EXCEL_PATH = "/opt/airflow/data/vente_billette.xlsx"
STAGING_PATH = "/opt/airflow/data/staging_ventes.parquet"
REPORT_PDF_PATH = "/opt/airflow/data/rapport_ventes.pdf"
DB_URL = "postgresql+psycopg2://elfouladh:elfouladh_pwd@postgres:5432/ventes_billettes"


def alerte_echec(context):
    """Envoie un email d'alerte quand une tâche échoue définitivement (retries épuisés)."""
    smtp_user = os.environ["SMTP_USER"]
    smtp_password = os.environ["SMTP_PASSWORD"]
    recipient = os.environ["REPORT_RECIPIENT"]

    ti = context["task_instance"]
    corps = f"""Alerte — échec du pipeline etl_vente_billette

Tâche en échec : {ti.task_id}
Run : {ti.run_id}
Date : {context.get('logical_date')}
Erreur : {context.get('exception')}

Voir les détails dans Airflow : http://localhost:8082
"""

    msg = MIMEMultipart()
    msg["From"] = smtp_user
    msg["To"] = recipient
    msg["Subject"] = f"[ALERTE] Échec du pipeline etl_vente_billette — {ti.task_id}"
    msg.attach(MIMEText(corps, "plain", "utf-8"))

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, recipient, msg.as_string())


@dag(
    dag_id="etl_vente_billette",
    schedule="0 6 * * 1",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 2, "on_failure_callback": alerte_echec},
    tags=["elfouladh", "ventes-billettes"],
)
def etl_vente_billette():

    @task
    def extract() -> str:
        """Lit le fichier Excel et nettoie les champs de base."""
        df = pd.read_excel(EXCEL_PATH)
        df.columns = [c.strip() for c in df.columns]

        df["RAISON_SOCIALE"] = df["RAISON_SOCIALE"].str.strip()
        df["RUE"] = df["RUE"].str.strip()
        df["DESIGNATION"] = df["DESIGNATION"].str.strip()
        df["MODEPAIEMENT"] = df["MODEPAIEMENT"].fillna("NON PRECISE")

        for col in ["TEL", "FAX"]:
            df[col] = df[col].apply(lambda v: str(int(v)) if pd.notna(v) else None)

        df["DATE_V"] = pd.to_datetime(df["DATE_V"]).dt.date.astype(str)

        df.to_parquet(STAGING_PATH, index=False)
        return STAGING_PATH

    @task
    def transform(staging_path: str) -> str:
        """Calcule un indicateur de vente simple (CA = prix * quantité)."""
        df = pd.read_parquet(staging_path)
        df["CA"] = df["PRIX"] * df["QUANTITE"]
        df.to_parquet(staging_path, index=False)
        return staging_path

    @task
    def load(staging_path: str) -> None:
        """Charge clients, articles et ventes dans PostgreSQL."""
        df = pd.read_parquet(staging_path)
        engine = create_engine(DB_URL)

        with engine.begin() as conn:
            clients = df[["RAISON_SOCIALE", "RUE", "TEL", "FAX"]].drop_duplicates(
                subset=["RAISON_SOCIALE"]
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

            articles = df[
                ["CODE_ARTICLE", "DESIGNATION", "FAMILLE", "SECTION", "UNITE_MESURE"]
            ].drop_duplicates(subset=["CODE_ARTICLE"])
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

            client_ids = dict(
                conn.execute(text("SELECT raison_sociale, client_id FROM dim_clients")).fetchall()
            )

            # On repart d'une table ventes propre à chaque exécution du DAG
            # (évite les doublons si on relance le run plusieurs fois avec le même fichier)
            conn.execute(text("TRUNCATE TABLE ventes RESTART IDENTITY"))

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

        print(f"Chargement terminé : {len(clients)} clients, {len(articles)} articles, {len(rows)} ventes.")

    @task
    def send_report() -> None:
        """Génère un PDF de synthèse et l'envoie par email au responsable."""
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            summary = conn.execute(text("""
                SELECT COALESCE(SUM(prix * quantite), 0) AS total_ca,
                       COUNT(*) AS total_ventes
                FROM ventes
            """)).mappings().first()

            top_clients = conn.execute(text("""
                SELECT c.raison_sociale, SUM(v.prix * v.quantite) AS ca
                FROM ventes v
                JOIN dim_clients c ON c.client_id = v.client_id
                GROUP BY c.raison_sociale
                ORDER BY ca DESC
                LIMIT 5
            """)).mappings().all()

            top_famille = conn.execute(text("""
                SELECT a.famille, SUM(v.prix * v.quantite) AS ca
                FROM ventes v
                JOIN dim_articles a ON a.code_article = v.code_article
                GROUP BY a.famille
                ORDER BY ca DESC
                LIMIT 1
            """)).mappings().first()

        date_rapport = pendulum.now("UTC").to_date_string()

        # --- Remarques générées par IA (Claude), avec repli sur des règles simples ---
        def remarques_par_regles():
            regles = []
            if top_clients:
                part_top1 = (top_clients[0].ca / summary.total_ca * 100) if summary.total_ca else 0
                regles.append(
                    f"{top_clients[0].raison_sociale} représente {part_top1:.0f} % du chiffre d'affaires total."
                )
            if top_famille:
                regles.append(f"La famille d'articles la plus vendue est \"{top_famille.famille}\".")
            if not regles:
                regles.append("Aucune remarque particulière cette semaine.")
            return regles

        def remarques_par_ia():
            api_key = os.environ.get("ANTHROPIC_API_KEY")
            if not api_key:
                return None
            try:
                import anthropic
                client = anthropic.Anthropic(api_key=api_key)

                lignes_top_clients = "\n".join(
                    f"- {row.raison_sociale} : {row.ca:.0f} DT" for row in top_clients
                )
                famille_txt = f"Famille la plus vendue : {top_famille.famille}" if top_famille else "N/A"

                prompt = f"""Voici les données de vente de la semaine pour une entreprise de sidérurgie :
Chiffre d'affaires total : {summary.total_ca:.0f} DT
Nombre de ventes : {summary.total_ventes}
Top 5 clients :
{lignes_top_clients}
{famille_txt}

Rédige entre 2 et 4 remarques courtes et concrètes (une phrase chacune, en français) à destination
d'un responsable commercial. Signale les points notables : concentration client, famille dominante,
tendances à surveiller. Ton professionnel et direct, pas de blabla.
Réponds uniquement avec les remarques, une par ligne, sans numérotation ni tirets."""

                message = client.messages.create(
                    model="claude-sonnet-4-5",
                    max_tokens=400,
                    messages=[{"role": "user", "content": prompt}],
                )
                texte = message.content[0].text.strip()
                lignes = [l.strip("- ").strip() for l in texte.split("\n") if l.strip()]
                return lignes if lignes else None
            except Exception as e:
                print(f"Remarques IA indisponibles ({e}), repli sur les règles simples.")
                return None

        remarques = remarques_par_ia() or remarques_par_regles()

        # --- Construction du PDF ---
        pdf = FPDF()
        pdf.add_page()

        pdf.set_font("Helvetica", "B", 18)
        pdf.cell(0, 12, "Rapport de synthese - Ventes de billettes", ln=True)
        pdf.set_font("Helvetica", "", 11)
        pdf.set_text_color(110, 110, 110)
        pdf.cell(0, 8, "Societe Tunisienne de Siderurgie - El Fouladh", ln=True)
        pdf.cell(0, 8, f"Genere le {date_rapport}", ln=True)
        pdf.ln(6)

        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", "B", 13)
        pdf.cell(0, 10, "Resume", ln=True)
        pdf.set_font("Helvetica", "", 12)
        pdf.cell(0, 8, f"Chiffre d'affaires total : {summary.total_ca:,.0f} DT".replace(",", " "), ln=True)
        pdf.cell(0, 8, f"Nombre de ventes : {summary.total_ventes}", ln=True)
        pdf.ln(6)

        pdf.set_font("Helvetica", "B", 13)
        pdf.cell(0, 10, "Top 5 clients", ln=True)
        pdf.set_font("Helvetica", "", 12)
        for i, row in enumerate(top_clients, start=1):
            ca_fmt = f"{row.ca:,.0f} DT".replace(",", " ")
            pdf.cell(0, 8, f"{i}. {row.raison_sociale} - {ca_fmt}", ln=True)
        pdf.ln(6)

        pdf.set_font("Helvetica", "B", 13)
        pdf.set_x(pdf.l_margin)
        pdf.cell(0, 10, "Remarques", ln=True)
        pdf.set_font("Helvetica", "", 12)
        usable_width = pdf.w - pdf.l_margin - pdf.r_margin
        for remarque in remarques:
            pdf.set_x(pdf.l_margin)
            pdf.multi_cell(usable_width, 8, f"- {remarque}")

        pdf.output(REPORT_PDF_PATH)

        # --- Envoi par email avec le PDF en pièce jointe ---
        smtp_user = os.environ["SMTP_USER"]
        smtp_password = os.environ["SMTP_PASSWORD"]
        recipient = os.environ["REPORT_RECIPIENT"]

        msg = MIMEMultipart()
        msg["From"] = smtp_user
        msg["To"] = recipient
        msg["Subject"] = f"Rapport hebdomadaire — Ventes de billettes ({date_rapport})"
        msg.attach(MIMEText(
            "Bonjour,\n\nVeuillez trouver ci-joint le rapport de synthèse hebdomadaire des ventes de billettes.\n\n"
            "Ce rapport a été généré automatiquement par le pipeline etl_vente_billette.",
            "plain", "utf-8",
        ))

        with open(REPORT_PDF_PATH, "rb") as f:
            piece_jointe = MIMEApplication(f.read(), _subtype="pdf")
            piece_jointe.add_header(
                "Content-Disposition", "attachment", filename="rapport_ventes.pdf"
            )
            msg.attach(piece_jointe)

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, recipient, msg.as_string())

        print(f"Rapport PDF envoyé à {recipient}")

    staging = extract()
    transformed = transform(staging)
    loaded = load(transformed)
    send_report().set_upstream(loaded)


etl_vente_billette()
