"""
DAG ETL - production (Aciérie, Laminoirs, Tréfilerie)
Extract  : lit production.xlsx
Transform: calcule un indicateur de rendement (100 - taux de rebut)
Load     : insère les ateliers et la production dans PostgreSQL
Report   : génère un PDF hebdomadaire par atelier et l'envoie par email

Remarque : les données de production sont générées à titre de démonstration
(les vraies données de production sont confidentielles).
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

EXCEL_PATH = "/opt/airflow/data/production.xlsx"
STAGING_PATH = "/opt/airflow/data/staging_production.parquet"
REPORT_PDF_PATH = "/opt/airflow/data/rapport_production.pdf"
CHART_TONNAGE_PATH = "/opt/airflow/data/chart_tonnage.png"
CHART_REBUT_PATH = "/opt/airflow/data/chart_rebut.png"
CHART_ARRET_PATH = "/opt/airflow/data/chart_arret.png"
DB_URL = "postgresql+psycopg2://elfouladh:elfouladh_pwd@postgres:5432/ventes_billettes"

# Une couleur fixe par atelier, réutilisée dans tous les graphiques du rapport
COULEURS_ATELIER = {
    "Aciérie": "#D9302A",
    "Laminoirs": "#5C7A8A",
    "Tréfilerie": "#2E8B78",
}
COULEUR_DEFAUT = "#8B9096"


def alerte_echec(context):
    smtp_user = os.environ["SMTP_USER"]
    smtp_password = os.environ["SMTP_PASSWORD"]
    recipient = os.environ["REPORT_RECIPIENT"]

    ti = context["task_instance"]
    corps = f"""Alerte — échec du pipeline etl_production

Tâche en échec : {ti.task_id}
Run : {ti.run_id}
Date : {context.get('logical_date')}
Erreur : {context.get('exception')}

Voir les détails dans Airflow : http://localhost:8082
"""
    msg = MIMEMultipart()
    msg["From"] = smtp_user
    msg["To"] = recipient
    msg["Subject"] = f"[ALERTE] Échec du pipeline etl_production — {ti.task_id}"
    msg.attach(MIMEText(corps, "plain", "utf-8"))

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, recipient, msg.as_string())


@dag(
    dag_id="etl_production",
    schedule="0 6 * * 1",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 2, "on_failure_callback": alerte_echec},
    tags=["elfouladh", "production"],
)
def etl_production():

    @task
    def extract() -> str:
        df = pd.read_excel(EXCEL_PATH)
        df.columns = [c.strip() for c in df.columns]
        df["ATELIER"] = df["ATELIER"].str.strip()
        df["PRODUIT"] = df["PRODUIT"].str.strip()
        df["DATE_PRODUCTION"] = pd.to_datetime(df["DATE_PRODUCTION"]).dt.date.astype(str)
        df.to_parquet(STAGING_PATH, index=False)
        return STAGING_PATH

    @task
    def transform(staging_path: str) -> str:
        """Calcule un indicateur de rendement (100 - taux de rebut)."""
        df = pd.read_parquet(staging_path)
        df["RENDEMENT"] = 100 - df["TAUX_REBUT"]
        df.to_parquet(staging_path, index=False)
        return staging_path

    @task
    def load(staging_path: str) -> None:
        df = pd.read_parquet(staging_path)
        engine = create_engine(DB_URL)

        with engine.begin() as conn:
            for atelier in df["ATELIER"].unique():
                conn.execute(
                    text("""
                        INSERT INTO dim_ateliers (nom) VALUES (:nom)
                        ON CONFLICT (nom) DO NOTHING
                    """),
                    {"nom": atelier},
                )

            atelier_ids = dict(
                conn.execute(text("SELECT nom, atelier_id FROM dim_ateliers")).fetchall()
            )

            # On repart d'une table propre à chaque exécution (même logique que les ventes)
            conn.execute(text("TRUNCATE TABLE production RESTART IDENTITY"))

            rows = [
                {
                    "atelier_id": atelier_ids[row.ATELIER],
                    "date": row.DATE_PRODUCTION,
                    "produit": row.PRODUIT,
                    "qte": float(row.QUANTITE_TONNES),
                    "rebut": float(row.TAUX_REBUT),
                    "arret": int(row.ARRET_MINUTES),
                }
                for row in df.itertuples(index=False)
            ]

            conn.execute(
                text("""
                    INSERT INTO production
                        (atelier_id, date_production, produit, quantite_tonnes, taux_rebut, arret_minutes)
                    VALUES
                        (:atelier_id, :date, :produit, :qte, :rebut, :arret)
                """),
                rows,
            )

        print(f"Chargement terminé : {len(atelier_ids)} ateliers, {len(rows)} lignes de production.")

    @task
    def send_report() -> None:
        """Génère un PDF hebdomadaire (7 derniers jours) avec graphiques et l'envoie par email."""
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            par_atelier = conn.execute(text("""
                SELECT a.nom,
                       SUM(p.quantite_tonnes) AS tonnes,
                       AVG(p.taux_rebut) AS rebut_moyen,
                       SUM(p.arret_minutes) AS arret_total
                FROM production p
                JOIN dim_ateliers a ON a.atelier_id = p.atelier_id
                WHERE p.date_production >= (SELECT MAX(date_production) FROM production) - INTERVAL '7 days'
                GROUP BY a.nom
                ORDER BY tonnes DESC
            """)).mappings().all()

            par_produit = conn.execute(text("""
                SELECT a.nom AS atelier, p.produit,
                       SUM(p.quantite_tonnes) AS tonnes,
                       AVG(p.taux_rebut) AS rebut_moyen,
                       SUM(p.arret_minutes) AS arret_total
                FROM production p
                JOIN dim_ateliers a ON a.atelier_id = p.atelier_id
                WHERE p.date_production >= (SELECT MAX(date_production) FROM production) - INTERVAL '7 days'
                GROUP BY a.nom, p.produit
                ORDER BY a.nom, tonnes DESC
            """)).mappings().all()

        date_rapport = pendulum.now("UTC").to_date_string()

        # --- Remarques générées par IA (Claude), avec repli sur des règles simples ---
        def remarques_par_regles():
            regles = []
            if par_atelier:
                plus_productif = max(par_atelier, key=lambda r: r.tonnes)
                regles.append(f"{plus_productif.nom} est l'atelier le plus productif cette semaine ({plus_productif.tonnes:,.0f} tonnes).".replace(",", " "))

                plus_rebut = max(par_atelier, key=lambda r: r.rebut_moyen)
                if plus_rebut.rebut_moyen > 3:
                    regles.append(f"Taux de rebut élevé à surveiller sur {plus_rebut.nom} ({plus_rebut.rebut_moyen:.1f} %).")

                plus_arret = max(par_atelier, key=lambda r: r.arret_total)
                if plus_arret.arret_total > 500:
                    regles.append(f"Temps d'arrêt notable sur {plus_arret.nom} ({plus_arret.arret_total} minutes cumulées).")
            if not regles:
                regles.append("Aucune anomalie particulière détectée cette semaine.")
            return regles

        def remarques_par_ia():
            api_key = os.environ.get("ANTHROPIC_API_KEY")
            if not api_key:
                return None
            try:
                import anthropic
                client = anthropic.Anthropic(api_key=api_key)

                lignes_donnees = "\n".join(
                    f"- {r.nom} : {r.tonnes:.0f} tonnes, taux de rebut moyen {r.rebut_moyen:.1f} %, "
                    f"arrêts cumulés {r.arret_total} minutes"
                    for r in par_atelier
                )
                prompt = f"""Voici les données de production de la semaine pour une usine sidérurgique (3 ateliers) :
{lignes_donnees}

Rédige entre 2 et 4 remarques courtes et concrètes (une phrase chacune, en français) à destination
d'un responsable de production. Signale les points notables : performance, rebut élevé, arrêts
machine, tendances à surveiller. Ton professionnel et direct, pas de blabla.
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

        # --- Graphiques (matplotlib, rendus en images puis insérés dans le PDF) ---
        def couleur(nom):
            return COULEURS_ATELIER.get(nom, COULEUR_DEFAUT)

        noms = [r.nom for r in par_atelier]
        couleurs = [couleur(n) for n in noms]
        graphiques_disponibles = bool(par_atelier) and sum(float(r.tonnes) for r in par_atelier) > 0

        if graphiques_disponibles:
            # 1. Anneau : répartition du tonnage par atelier
            plt.figure(figsize=(3.4, 3.4))
            plt.pie(
                [max(float(r.tonnes), 0.01) for r in par_atelier], labels=noms, colors=couleurs,
                autopct="%1.0f%%", pctdistance=0.8, startangle=90,
                wedgeprops={"width": 0.38, "edgecolor": "white"},
                textprops={"fontsize": 9, "color": "#1F2421"},
            )
            plt.title("Tonnage par atelier", fontsize=10, color="#1F2421")
            plt.tight_layout()
            plt.savefig(CHART_TONNAGE_PATH, dpi=160, transparent=True)
            plt.close()

            # 2. Barres horizontales : taux de rebut moyen par atelier
            plt.figure(figsize=(3.8, 2.6))
            plt.barh(noms, [float(r.rebut_moyen) for r in par_atelier], color=couleurs, height=0.5)
            plt.xlabel("Taux de rebut moyen (%)", fontsize=9, color="#1F2421")
            plt.gca().invert_yaxis()
            plt.gca().spines[["top", "right"]].set_visible(False)
            plt.tick_params(labelsize=9, colors="#1F2421")
            plt.tight_layout()
            plt.savefig(CHART_REBUT_PATH, dpi=160, transparent=True)
            plt.close()

            # 3. Anneau : répartition des arrêts machine par atelier
            plt.figure(figsize=(3.4, 3.4))
            plt.pie(
                [max(float(r.arret_total), 0.01) for r in par_atelier], labels=noms, colors=couleurs,
                autopct="%1.0f%%", pctdistance=0.8, startangle=90,
                wedgeprops={"width": 0.38, "edgecolor": "white"},
                textprops={"fontsize": 9, "color": "#1F2421"},
            )
            plt.title("Arrêts machine par atelier", fontsize=10, color="#1F2421")
            plt.tight_layout()
            plt.savefig(CHART_ARRET_PATH, dpi=160, transparent=True)
            plt.close()

        # --- Construction du PDF ---
        pdf = FPDF()
        pdf.add_page()
        page_w = pdf.w - pdf.l_margin - pdf.r_margin

        # Bandeau d'en-tête
        pdf.set_fill_color(29, 34, 38)  # graphite foncé (identique au tableau de bord)
        pdf.rect(0, 0, pdf.w, 32, style="F")

        LOGO_PATH = "/opt/airflow/data/logo.png"
        if os.path.exists(LOGO_PATH):
            pdf.image(LOGO_PATH, x=pdf.w - pdf.r_margin - 20, y=6, w=20)

        pdf.set_text_color(255, 255, 255)
        pdf.set_xy(pdf.l_margin, 8)
        pdf.set_font("Helvetica", "B", 18)
        pdf.cell(0, 10, "Rapport hebdomadaire - Production", ln=True)
        pdf.set_x(pdf.l_margin)
        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(200, 200, 200)
        pdf.cell(0, 6, f"Societe Tunisienne de Siderurgie - El Fouladh | {date_rapport} | 7 derniers jours", ln=True)
        pdf.ln(6)
        pdf.set_text_color(0, 0, 0)

        # Bande de KPI (4 cases colorées)
        kpi_y = pdf.get_y() + 4
        total_tonnes = sum(r.tonnes for r in par_atelier) if par_atelier else 0
        total_arret = sum(r.arret_total for r in par_atelier) if par_atelier else 0
        rebut_moyen_global = (sum(r.rebut_moyen for r in par_atelier) / len(par_atelier)) if par_atelier else 0
        kpis = [
            ("Tonnage total", f"{total_tonnes:,.0f} t".replace(",", " "), "#D9302A"),
            ("Rebut moyen", f"{rebut_moyen_global:.1f} %", "#5C7A8A"),
            ("Arrets cumules", f"{total_arret} min", "#2E8B78"),
            ("Ateliers suivis", f"{len(par_atelier)}", "#8B9096"),
        ]
        box_w = page_w / 4 - 3
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

        # Tableau détaillé par atelier / produit
        pdf.set_font("Helvetica", "B", 13)
        pdf.set_x(pdf.l_margin)
        pdf.cell(0, 10, "Detail par atelier et produit", ln=True)
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_fill_color(232, 231, 226)
        pdf.set_x(pdf.l_margin)
        col_widths = [45, 45, 30, 30, 30]
        headers = ["Atelier", "Produit", "Tonnes", "Rebut moy.", "Arrets (min)"]
        for w, h in zip(col_widths, headers):
            pdf.cell(w, 8, h, border=0, fill=True)
        pdf.ln(8)
        pdf.set_font("Helvetica", "", 10)
        for row in par_produit:
            pdf.set_x(pdf.l_margin)
            pdf.cell(col_widths[0], 7, row.atelier, border="B")
            pdf.cell(col_widths[1], 7, row.produit, border="B")
            pdf.cell(col_widths[2], 7, f"{row.tonnes:,.0f}".replace(",", " "), border="B")
            pdf.cell(col_widths[3], 7, f"{row.rebut_moyen:.1f} %", border="B")
            pdf.cell(col_widths[4], 7, f"{row.arret_total}", border="B")
            pdf.ln(7)
        pdf.ln(6)

        # Graphiques : 2 anneaux + 1 barre, sur une nouvelle page si besoin de place
        if pdf.get_y() > pdf.h - 100:
            pdf.add_page()
        chart_y = pdf.get_y() + 2
        chart_w = page_w / 3 - 4
        if graphiques_disponibles:
            pdf.image(CHART_TONNAGE_PATH, x=pdf.l_margin, y=chart_y, w=chart_w)
            pdf.image(CHART_REBUT_PATH, x=pdf.l_margin + chart_w + 6, y=chart_y + 22, w=chart_w)
            pdf.image(CHART_ARRET_PATH, x=pdf.l_margin + 2 * (chart_w + 6), y=chart_y, w=chart_w)
            pdf.set_y(chart_y + chart_w + 10)
        else:
            pdf.set_x(pdf.l_margin)
            pdf.set_font("Helvetica", "I", 11)
            pdf.set_text_color(107, 111, 107)
            pdf.cell(0, 10, "Aucune donnee de production sur les 7 derniers jours.", ln=True)
            pdf.set_text_color(0, 0, 0)
            pdf.ln(4)

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

        # --- Envoi par email ---
        smtp_user = os.environ["SMTP_USER"]
        smtp_password = os.environ["SMTP_PASSWORD"]
        recipient = os.environ["REPORT_RECIPIENT"]

        msg = MIMEMultipart()
        msg["From"] = smtp_user
        msg["To"] = recipient
        msg["Subject"] = f"Rapport hebdomadaire — Production ({date_rapport})"
        msg.attach(MIMEText(
            "Bonjour,\n\nVeuillez trouver ci-joint le rapport hebdomadaire de production "
            "(Aciérie, Laminoirs, Tréfilerie).\n\n"
            "Ce rapport a été généré automatiquement par le pipeline etl_production.",
            "plain", "utf-8",
        ))

        with open(REPORT_PDF_PATH, "rb") as f:
            piece_jointe = MIMEApplication(f.read(), _subtype="pdf")
            piece_jointe.add_header("Content-Disposition", "attachment", filename="rapport_production.pdf")
            msg.attach(piece_jointe)

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, recipient, msg.as_string())

        print(f"Rapport production envoyé à {recipient}")

    staging = extract()
    transformed = transform(staging)
    loaded = load(transformed)
    send_report().set_upstream(loaded)


etl_production()
