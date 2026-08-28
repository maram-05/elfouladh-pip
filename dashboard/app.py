"""
Tableau de bord — Pipeline El Fouladh
Page web indépendante avec deux volets :
  - Ventes de billettes (base ventes_billettes)
  - Production par atelier : Aciérie, Laminoirs, Tréfilerie (base ventes_billettes, tables production)
  - Statut des pipelines Airflow (base de métadonnées airflow)
  - Un espace "documents" (archive) et un espace "source de données" par volet
"""

import os
from datetime import datetime

from flask import Flask, jsonify, render_template, request, send_from_directory, abort
from werkzeug.utils import secure_filename
from sqlalchemy import create_engine, text

app = Flask(__name__)

SALES_DB_URL = "postgresql+psycopg2://elfouladh:elfouladh_pwd@postgres:5432/ventes_billettes"
AIRFLOW_DB_URL = "postgresql+psycopg2://airflow:airflow@airflow-postgres:5432/airflow"

# --- Espace "documents" (archive, ne nourrit pas le pipeline) ---
DOCS_BASE = os.path.join(os.path.dirname(__file__), "documents")
DOCS_CATEGORIES = ("ventes", "production")
ALLOWED_EXTENSIONS = {"pdf", "xlsx", "xls", "csv", "docx", "png", "jpg", "jpeg"}

for cat in DOCS_CATEGORIES:
    os.makedirs(os.path.join(DOCS_BASE, cat), exist_ok=True)

# --- Fichiers sources lus par les pipelines Airflow (dossier partagé ./data) ---
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
SOURCE_FILES = {
    "ventes": "vente_billette.xlsx",
    "production": "production.xlsx",
}
os.makedirs(DATA_DIR, exist_ok=True)

sales_engine = create_engine(SALES_DB_URL)
airflow_engine = create_engine(AIRFLOW_DB_URL)


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def fetch_pipeline_status(dag_id: str):
    with airflow_engine.connect() as conn:
        last_run = conn.execute(text("""
            SELECT run_id, state, start_date, end_date, run_type
            FROM dag_run
            WHERE dag_id = :dag_id
            ORDER BY start_date DESC NULLS LAST
            LIMIT 1
        """), {"dag_id": dag_id}).mappings().first()

        history = conn.execute(text("""
            SELECT run_id, state, start_date, end_date, run_type
            FROM dag_run
            WHERE dag_id = :dag_id
            ORDER BY start_date DESC NULLS LAST
            LIMIT 10
        """), {"dag_id": dag_id}).mappings().all()

    def fmt(r):
        if r is None:
            return None
        d = dict(r)
        for k in ("start_date", "end_date"):
            if d.get(k) is not None:
                d[k] = d[k].isoformat()
        return d

    return {"last_run": fmt(last_run), "history": [fmt(r) for r in history]}


# --- Pages ---

@app.route("/")
def accueil():
    return render_template("accueil.html")


@app.route("/ventes")
def page_ventes():
    return render_template("ventes.html")


@app.route("/production")
def page_choix_production():
    with sales_engine.connect() as conn:
        ateliers = conn.execute(text("SELECT nom FROM dim_ateliers ORDER BY nom")).scalars().all()
    return render_template("production_choix.html", ateliers=ateliers)


@app.route("/production/<atelier>")
def page_production(atelier):
    return render_template("production.html", atelier=atelier)


# --- API Ventes ---

@app.route("/api/sales-summary")
def sales_summary():
    with sales_engine.connect() as conn:
        row = conn.execute(text("""
            SELECT
                COALESCE(SUM(prix * quantite), 0) AS total_ca,
                COUNT(*) AS total_ventes,
                (SELECT COUNT(*) FROM dim_clients) AS total_clients,
                (SELECT COUNT(*) FROM dim_articles) AS total_articles
            FROM ventes
        """)).mappings().first()
    return jsonify(dict(row))


@app.route("/api/sales-by-month")
def sales_by_month():
    with sales_engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT to_char(date_trunc('month', date_vente), 'YYYY-MM') AS mois,
                   SUM(prix * quantite) AS ca
            FROM ventes
            GROUP BY 1
            ORDER BY 1
        """)).mappings().all()
    return jsonify([dict(r) for r in rows])


@app.route("/api/top-clients")
def top_clients():
    with sales_engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT c.raison_sociale, SUM(v.prix * v.quantite) AS ca
            FROM ventes v
            JOIN dim_clients c ON c.client_id = v.client_id
            GROUP BY c.raison_sociale
            ORDER BY ca DESC
            LIMIT 8
        """)).mappings().all()
    return jsonify([dict(r) for r in rows])


@app.route("/api/sales-by-family")
def sales_by_family():
    with sales_engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT a.famille, SUM(v.prix * v.quantite) AS ca
            FROM ventes v
            JOIN dim_articles a ON a.code_article = v.code_article
            GROUP BY a.famille
            ORDER BY ca DESC
        """)).mappings().all()
    return jsonify([dict(r) for r in rows])


@app.route("/api/pipeline-status")
def pipeline_status():
    return jsonify(fetch_pipeline_status("etl_vente_billette"))


# --- API Production ---

@app.route("/api/production-ateliers")
def production_ateliers():
    with sales_engine.connect() as conn:
        rows = conn.execute(text("SELECT nom FROM dim_ateliers ORDER BY nom")).scalars().all()
    return jsonify(rows)


@app.route("/api/production-summary/<atelier>")
def production_summary(atelier):
    with sales_engine.connect() as conn:
        row = conn.execute(text("""
            SELECT
                COALESCE(SUM(p.quantite_tonnes), 0) AS total_tonnes,
                COALESCE(AVG(p.taux_rebut), 0) AS rebut_moyen,
                COALESCE(SUM(p.arret_minutes), 0) AS arret_total,
                COUNT(DISTINCT p.produit) AS nb_produits
            FROM production p
            JOIN dim_ateliers a ON a.atelier_id = p.atelier_id
            WHERE a.nom = :atelier
        """), {"atelier": atelier}).mappings().first()
    return jsonify(dict(row))


@app.route("/api/production-by-week/<atelier>")
def production_by_week(atelier):
    with sales_engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT to_char(date_trunc('week', p.date_production), 'YYYY-MM-DD') AS semaine,
                   SUM(p.quantite_tonnes) AS tonnes
            FROM production p
            JOIN dim_ateliers a ON a.atelier_id = p.atelier_id
            WHERE a.nom = :atelier
            GROUP BY 1
            ORDER BY 1
            OFFSET GREATEST(0, (
                SELECT COUNT(DISTINCT date_trunc('week', p2.date_production))
                FROM production p2 JOIN dim_ateliers a2 ON a2.atelier_id = p2.atelier_id
                WHERE a2.nom = :atelier
            ) - 12)
        """), {"atelier": atelier}).mappings().all()
    return jsonify([dict(r) for r in rows])


@app.route("/api/production-by-product/<atelier>")
def production_by_product(atelier):
    with sales_engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT p.produit, SUM(p.quantite_tonnes) AS tonnes
            FROM production p
            JOIN dim_ateliers a ON a.atelier_id = p.atelier_id
            WHERE a.nom = :atelier
            GROUP BY p.produit
            ORDER BY tonnes DESC
        """), {"atelier": atelier}).mappings().all()
    return jsonify([dict(r) for r in rows])


@app.route("/api/production-pipeline-status")
def production_pipeline_status():
    return jsonify(fetch_pipeline_status("etl_production"))


# --- Espace documents (archive — ventes ou production) ---

def _docs_dir(category):
    if category not in DOCS_CATEGORIES:
        abort(404)
    return os.path.join(DOCS_BASE, category)


@app.route("/api/documents/<category>", methods=["GET"])
def list_documents(category):
    docs_dir = _docs_dir(category)
    files = []
    for name in sorted(os.listdir(docs_dir)):
        path = os.path.join(docs_dir, name)
        if os.path.isfile(path):
            stat = os.stat(path)
            files.append({
                "name": name,
                "size": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            })
    files.sort(key=lambda f: f["modified"], reverse=True)
    return jsonify(files)


@app.route("/api/documents/<category>", methods=["POST"])
def upload_document(category):
    docs_dir = _docs_dir(category)

    if "file" not in request.files:
        return jsonify({"error": "Aucun fichier reçu."}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "Nom de fichier vide."}), 400

    if not allowed_file(file.filename):
        return jsonify({"error": "Type de fichier non autorisé."}), 400

    filename = secure_filename(file.filename)
    dest = os.path.join(docs_dir, filename)

    base, ext = os.path.splitext(filename)
    counter = 1
    while os.path.exists(dest):
        filename = f"{base}_{counter}{ext}"
        dest = os.path.join(docs_dir, filename)
        counter += 1

    file.save(dest)
    return jsonify({"name": filename}), 201


@app.route("/api/documents/<category>/<path:filename>", methods=["GET"])
def download_document(category, filename):
    docs_dir = _docs_dir(category)
    safe_name = secure_filename(filename)
    if not os.path.exists(os.path.join(docs_dir, safe_name)):
        abort(404)
    return send_from_directory(docs_dir, safe_name, as_attachment=True)


@app.route("/api/documents/<category>/<path:filename>", methods=["DELETE"])
def delete_document(category, filename):
    docs_dir = _docs_dir(category)
    safe_name = secure_filename(filename)
    path = os.path.join(docs_dir, safe_name)
    if not os.path.exists(path):
        abort(404)
    os.remove(path)
    return jsonify({"deleted": safe_name})


# --- Mise à jour du fichier source (celui que le pipeline lit réellement) ---

@app.route("/api/source/<category>", methods=["GET"])
def source_info(category):
    if category not in SOURCE_FILES:
        abort(404)
    path = os.path.join(DATA_DIR, SOURCE_FILES[category])
    if not os.path.exists(path):
        return jsonify({"exists": False})
    stat = os.stat(path)
    return jsonify({
        "exists": True,
        "filename": SOURCE_FILES[category],
        "size": stat.st_size,
        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
    })


@app.route("/api/source/<category>", methods=["POST"])
def replace_source(category):
    if category not in SOURCE_FILES:
        abort(404)

    if "file" not in request.files:
        return jsonify({"error": "Aucun fichier reçu."}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "Nom de fichier vide."}), 400

    if not file.filename.lower().endswith(".xlsx"):
        return jsonify({"error": "Seuls les fichiers .xlsx sont acceptés ici."}), 400

    dest = os.path.join(DATA_DIR, SOURCE_FILES[category])
    file.save(dest)
    return jsonify({"filename": SOURCE_FILES[category], "message": "Fichier source remplacé."}), 201


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
