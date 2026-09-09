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
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from fpdf import FPDF
from sqlalchemy import create_engine, text

from airflow.sdk import dag, task

EXCEL_PATH = "/opt/airflow/data/vente_billette.xlsx"
STAGING_PATH = "/opt/airflow/data/staging_ventes.parquet"
REPORT_PDF_PATH = "/opt/airflow/data/rapport_ventes.pdf"
CHART_SECTION_PATH = "/opt/airflow/data/chart_sections.png"
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

            top_section = conn.execute(text("""
                SELECT a.section, SUM(v.prix * v.quantite) AS ca
                FROM ventes v
                JOIN dim_articles a ON a.code_article = v.code_article
                GROUP BY a.section
                ORDER BY ca DESC
                LIMIT 1
            """)).mappings().first()

            par_section = conn.execute(text("""
                SELECT a.section,
                       SUM(v.prix * v.quantite) AS ca,
                       COUNT(*) AS nb_ventes,
                       COUNT(DISTINCT v.client_id) AS nb_clients
                FROM ventes v
                JOIN dim_articles a ON a.code_article = v.code_article
                GROUP BY a.section
                ORDER BY ca DESC
            """)).mappings().all()

            top_client_par_section = {}
            for row in par_section:
                tc = conn.execute(text("""
                    SELECT c.raison_sociale, SUM(v.prix * v.quantite) AS ca
                    FROM ventes v
                    JOIN dim_articles a ON a.code_article = v.code_article
                    JOIN dim_clients c ON c.client_id = v.client_id
                    WHERE a.section = :section
                    GROUP BY c.raison_sociale
                    ORDER BY ca DESC
                    LIMIT 1
                """), {"section": row.section}).mappings().first()
                top_client_par_section[row.section] = tc.raison_sociale if tc else "—"

        LABELS_SECTION = {
            "SECTION RAB": "Rond à béton",
            "TREFILES": "Fils",
            "STRUCTURE METALLIQUE": "Structures métaliques",
            "ROND MARCHAND & DIVERS": "Rond marchand & divers",
            "BILLETTE": "Billette",
        }
        COULEURS_SECTION = {
            "SECTION RAB": "#D9302A",
            "TREFILES": "#5C7A8A",
            "STRUCTURE METALLIQUE": "#2E8B78",
            "ROND MARCHAND & DIVERS": "#8B9096",
            "BILLETTE": "#C9A227",
        }
        top_section_label = LABELS_SECTION.get(top_section.section, top_section.section) if top_section else None

        date_rapport = pendulum.now("UTC").to_date_string()

        # --- Remarques générées par IA (Claude), avec repli sur des règles simples ---
        def remarques_par_regles():
            regles = []
            if top_clients:
                part_top1 = (top_clients[0].ca / summary.total_ca * 100) if summary.total_ca else 0
                regles.append(
                    f"{top_clients[0].raison_sociale} représente {part_top1:.0f} % du chiffre d'affaires total."
                )
            if top_section_label:
                regles.append(f"Le type de produit le plus vendu est \"{top_section_label}\".")
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
                famille_txt = f"Type de produit le plus vendu : {top_section_label}" if top_section_label else "N/A"

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

        # --- Graphique : répartition du CA par type de produit ---
        labels_graph = [LABELS_SECTION.get(r.section, r.section) for r in par_section]
        couleurs_graph = [COULEURS_SECTION.get(r.section, "#8B9096") for r in par_section]
        plt.figure(figsize=(4, 4))
        plt.pie(
            [float(r.ca) for r in par_section], labels=labels_graph, colors=couleurs_graph,
            autopct="%1.0f%%", pctdistance=0.8, startangle=90,
            wedgeprops={"width": 0.38, "edgecolor": "white"},
            textprops={"fontsize": 9, "color": "#1F2421"},
        )
        plt.title("Répartition du CA par produit", fontsize=11, color="#1F2421")
        plt.tight_layout()
        plt.savefig(CHART_SECTION_PATH, dpi=160, transparent=True)
        plt.close()

        # --- Construction du PDF ---
        pdf = FPDF()
        pdf.add_page()
        page_w = pdf.w - pdf.l_margin - pdf.r_margin

        LOGO_PATH = "/opt/airflow/data/logo.png"
        if os.path.exists(LOGO_PATH):
            pdf.image(LOGO_PATH, x=pdf.l_margin, y=10, w=18)
            texte_x = pdf.l_margin + 24
        else:
            texte_x = pdf.l_margin

        pdf.set_xy(texte_x, 10)
        pdf.set_font("Helvetica", "B", 18)
        pdf.cell(0, 12, "Rapport de synthese - Ventes", ln=True)
        pdf.set_x(texte_x)
        pdf.set_font("Helvetica", "", 11)
        pdf.set_text_color(110, 110, 110)
        pdf.cell(0, 8, "Societe Tunisienne de Siderurgie - El Fouladh", ln=True)
        pdf.set_x(texte_x)
        pdf.cell(0, 8, f"Genere le {date_rapport}", ln=True)
        pdf.ln(6)
        pdf.set_text_color(0, 0, 0)

        # Bande de KPI globale
        kpi_y = pdf.get_y() + 2
        kpis = [
            ("Chiffre d'affaires", f"{summary.total_ca:,.0f} DT".replace(",", " "), "#D9302A"),
            ("Ventes", f"{summary.total_ventes:,.0f}".replace(",", " "), "#5C7A8A"),
            ("Types de produits", f"{len(par_section)}", "#2E8B78"),
        ]
        box_w = page_w / 3 - 3
        x = pdf.l_margin
        for label, value, hexcolor in kpis:
            r, g, b = tuple(int(hexcolor.lstrip("#")[i:i+2], 16) for i in (0, 2, 4))
            pdf.set_fill_color(245, 243, 238)
            pdf.rect(x, kpi_y, box_w, 22, style="F")
            pdf.set_fill_color(r, g, b)
            pdf.rect(x, kpi_y, 3, 22, style="F")
            pdf.set_xy(x + 6, kpi_y + 3)
            pdf.set_font("Helvetica", "", 8)
            pdf.set_text_color(107, 111, 107)
            pdf.cell(box_w - 8, 5, label.upper())
            pdf.set_xy(x + 6, kpi_y + 10)
            pdf.set_font("Helvetica", "B", 13)
            pdf.set_text_color(31, 36, 33)
            pdf.cell(box_w - 8, 8, value)
            x += box_w + 4
        pdf.set_y(kpi_y + 28)

        # Détail indépendant par produit
        pdf.set_font("Helvetica", "B", 13)
        pdf.set_x(pdf.l_margin)
        pdf.cell(0, 10, "Detail par produit", ln=True)
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_fill_color(232, 231, 226)
        pdf.set_x(pdf.l_margin)
        col_widths = [55, 35, 30, 60]
        headers = ["Produit", "CA (DT)", "Ventes", "Meilleur client"]
        for w, h in zip(col_widths, headers):
            pdf.cell(w, 8, h, border=0, fill=True)
        pdf.ln(8)
        pdf.set_font("Helvetica", "", 10)
        for row in par_section:
            label = LABELS_SECTION.get(row.section, row.section)
            meilleur = top_client_par_section.get(row.section, "—")
            meilleur_court = meilleur if len(meilleur) <= 32 else meilleur[:30] + "…"
            pdf.set_x(pdf.l_margin)
            pdf.cell(col_widths[0], 7, label, border="B")
            pdf.cell(col_widths[1], 7, f"{row.ca:,.0f}".replace(",", " "), border="B")
            pdf.cell(col_widths[2], 7, f"{row.nb_ventes}", border="B")
            pdf.cell(col_widths[3], 7, meilleur_court, border="B")
            pdf.ln(7)
        pdf.ln(6)

        # Graphique de répartition + Top 5 clients, côte à côte
        graph_y = pdf.get_y()
        graph_w = page_w * 0.42
        pdf.image(CHART_SECTION_PATH, x=pdf.l_margin, y=graph_y, w=graph_w)

        table_x = pdf.l_margin + graph_w + 10
        pdf.set_xy(table_x, graph_y)
        pdf.set_font("Helvetica", "B", 13)
        pdf.cell(0, 10, "Top 5 clients", ln=True)
        pdf.set_font("Helvetica", "", 11)
        for i, row in enumerate(top_clients, start=1):
            ca_fmt = f"{row.ca:,.0f} DT".replace(",", " ")
            pdf.set_x(table_x)
            pdf.cell(0, 8, f"{i}. {row.raison_sociale} - {ca_fmt}", ln=True)
        pdf.set_y(max(pdf.get_y(), graph_y + graph_w) + 8)

        # Remarques
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
