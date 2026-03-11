# Notion Importer — HEC IA Intelligence

Importe des fichiers CSV/XLSX vers ta base Notion Prospection.

## Structure

```
notion-importer/
├── app.py              ← Flask backend (API + serve le frontend)
├── templates/
│   └── index.html      ← Frontend (OpenAI-style)
├── requirements.txt
├── Procfile
└── railway.toml
```

## Déploiement sur Railway (5 min)

### 1. Prépare ton repo Git

```bash
cd notion-importer
git init
git add .
git commit -m "init"
```

### 2. Pousse sur GitHub

Crée un repo sur github.com, puis :

```bash
git remote add origin https://github.com/TON_USER/notion-importer.git
git push -u origin main
```

### 3. Déploie sur Railway

1. Va sur [railway.app](https://railway.app) → **New Project → Deploy from GitHub**
2. Sélectionne ton repo `notion-importer`
3. Railway détecte automatiquement Python + Procfile
4. Dans **Settings → Variables**, ajoute si tu veux fixer le token côté serveur :
   ```
   NOTION_DATABASE_ID = e243602070b1493fad77a2cf0c24f54b
   PORT = 8080  (Railway le gère automatiquement)
   ```
5. **Deploy** → ton app est live sur `https://ton-app.up.railway.app`

### 4. (Optionnel) Domaine custom

Dans Railway → Settings → Networking → Custom Domain.

---

## Run local

```bash
pip install -r requirements.txt
python app.py
# → http://localhost:5000
```

---

## Token Notion

1. Va sur [notion.so/my-integrations](https://www.notion.so/my-integrations)
2. **New integration** → donne-lui un nom (ex: "HEC IA Importer")
3. Copie le token `secret_xxx`
4. Dans Notion, ouvre ta base **Prospection** → **...** → **Add connections** → sélectionne ton intégration

---

## Colonnes CSV/XLSX reconnues

| Colonne Notion           | Type      | Notes                                   |
|--------------------------|-----------|-----------------------------------------|
| Nom de l'entreprise      | title     | **Obligatoire**                         |
| Statut du Partenariat    | select    | En Cours · Demande · Done               |
| Type d'entreprise        | select    | PE · VC · Cabinet de conseil · Startup · Grand Groupe · Association |
| Prénom-Nom contact       | text      |                                         |
| Poste contact            | text      |                                         |
| Mail contact             | email     |                                         |
| Numéro contact           | phone     |                                         |
| Objet de la demande      | text      |                                         |
| Date d'envoi             | date      | Format YYYY-MM-DD ou Excel date         |
| Notes                    | text      |                                         |

Les en-têtes sont normalisés automatiquement (accents, casse, espaces).
