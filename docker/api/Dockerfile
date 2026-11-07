# API REST de lecture de la couche gold (FastAPI -> Trino).
# Contexte de build : racine du repo (comme les autres images) ; code dans api/.
FROM python:3.11.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /srv
COPY api/requirements.txt api/requirements-dev.txt api/constraints.txt ./
# Les dépendances de test (pytest, httpx) sont légères : on les embarque pour pouvoir
# lancer les tests dans le conteneur, sans environnement Python sur le poste.
RUN pip install -r requirements.txt -r requirements-dev.txt -c constraints.txt

COPY api/app ./app
COPY api/tests ./tests

# Utilisateur non-root.
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin api
USER 10001

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
