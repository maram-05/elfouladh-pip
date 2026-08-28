# Étape 1 — Import du fichier de vente de billettes dans PostgreSQL

Ce dossier contient tout ce qu'il faut pour lancer une base PostgreSQL via Docker et y importer
le fichier `vente_billette.xlsx`, avant d'automatiser tout ça avec Airflow (étapes suivantes).

## Contenu

- `docker-compose.yml` — lance PostgreSQL + Adminer (interface web pour visualiser la base)
- `sql/schema.sql` — création des 3 tables (`dim_clients`, `dim_articles`, `ventes`), exécuté
  automatiquement au premier démarrage du conteneur
- `import_data.py` — script Python qui lit l'Excel, nettoie les données et les insère en base
- `requirements.txt` — dépendances Python

## Schéma retenu

Le fichier Excel est une table "à plat" (une ligne = une vente, avec les infos client et article
répétées). Pour que ce soit exploitable en base, les données sont éclatées en 3 tables :

- **dim_clients** : un client par `RAISON_SOCIALE` (298 clients distincts)
- **dim_articles** : un article par `CODE_ARTICLE` (80 articles distincts) — vérifié : chaque code
  article a toujours la même désignation/famille/section/unité, donc pas d'incohérence
- **ventes** : la table de faits, une ligne par vente, avec des clés étrangères vers les deux
  premières tables

## Lancer l'environnement

```bash
cd elfouladh-pipeline
docker compose up -d
```

Cela démarre :
- PostgreSQL sur `localhost:5432` (base `ventes_billettes`, utilisateur `elfouladh`, mot de passe
  `elfouladh_pwd`)
- Adminer sur http://localhost:8080 (système : PostgreSQL, serveur : `postgres`, utilisateur :
  `elfouladh`, mot de passe : `elfouladh_pwd`, base : `ventes_billettes`)

Sous Windows, lance ces commandes dans WSL2 (comme prévu dans la fiche projet).

## Importer les données

```bash
pip install -r requirements.txt
python import_data.py vente_billette.xlsx
```

Le script :
1. lit le fichier Excel avec pandas
2. nettoie les champs (espaces, `TEL`/`FAX` manquants, `MODEPAIEMENT` manquant → `NON PRECISE`)
3. insère les clients et articles uniques (`ON CONFLICT ... DO UPDATE`, donc rejouable sans
   doublons)
4. insère les 4096 lignes de vente en les reliant aux clients et articles

## Vérifier l'import

Dans Adminer ou avec `psql` :

```sql
SELECT count(*) FROM ventes;                 -- doit renvoyer 4096
SELECT count(*) FROM dim_clients;             -- doit renvoyer 298
SELECT count(*) FROM dim_articles;            -- doit renvoyer 80

-- exemple : chiffre d'affaires par client
SELECT c.raison_sociale, sum(v.prix * v.quantite) AS ca
FROM ventes v
JOIN dim_clients c ON c.client_id = v.client_id
GROUP BY c.raison_sociale
ORDER BY ca DESC
LIMIT 10;
```

## Prochaine étape

Une fois cet import manuel validé, l'étape suivante (semaine 2 du plan) consiste à transformer
ce script en DAG Airflow, pour que l'extraction/nettoyage/chargement se fasse automatiquement.
