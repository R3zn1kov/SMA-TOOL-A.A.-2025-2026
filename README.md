# SMA TOOL A.A. 2025-2026

Applicazione Streamlit per l'analisi di contenuti da Google News e Reddit. Questa applicazione fornisce strumenti per raccogliere e analizzare dati testuali da queste due piattaforme per scopi di ricerca e studio.

## 🚀 Guida all'installazione completa

### Prerequisiti

#### 1. Installazione Python
- Scarica Python 3.8+ da [python.org](https://www.python.org/downloads/)
- Durante l'installazione, assicurati di selezionare "Add Python to PATH"
- Verifica l'installazione aprendo il terminale e digitando: `python --version`

#### 2. Installazione Git
- Scarica Git da [git-scm.com](https://git-scm.com/downloads)
- Durante l'installazione su Windows, mantieni le impostazioni predefinite
- Verifica l'installazione aprendo il terminale e digitando: `git --version`

#### 3. Installazione PyCharm Community Edition
- Scarica PyCharm Community Edition da [jetbrains.com](https://www.jetbrains.com/pycharm/download/)
- Installa seguendo le istruzioni del sistema operativo

### Configurazione del progetto

#### 4. Creazione progetto in PyCharm
1. Apri PyCharm Community Edition
2. Clicca su "New Project"
3. Seleziona la cartella dove vuoi creare il progetto
4. Assicurati che l'interprete Python sia correttamente impostato

#### 5. Importazione del codice
**Opzione A - Da GitHub (consigliata):**
1. In PyCharm: VCS → Get from Version Control
2. Inserisci l'URL della repository: `https://github.com/R3zn1kov/SMA-TOOL-A.A.-2025-2026.git`
3. Scegli la cartella di destinazione
4. Clicca "Clone"

**Opzione B - Da file ZIP:**
1. Scarica il file ZIP della repository
2. Estrai il contenuto in una cartella
3. In PyCharm: File → Open → Seleziona la cartella estratta

### Configurazione dell'ambiente

#### 6. Creazione dell'ambiente virtuale
Apri il terminale in PyCharm (View → Tool Windows → Terminal) ed esegui:

```bash
# Creazione ambiente virtuale
python -m venv venv

# Attivazione ambiente virtuale
# Su Windows:
venv\Scripts\activate
# Su macOS/Linux:
source venv/bin/activate
```

#### 7. Installazione dipendenze
Con l'ambiente virtuale attivato, installa le dipendenze:

```bash
pip install -r requirements.txt
```

#### 8. Avvio dell'applicazione
Per avviare l'applicazione Streamlit:

```bash
streamlit run test.py
```

L'applicazione sarà disponibile nel browser all'indirizzo:
👉 **http://localhost:8502**

---

## 📦 Tecnologie utilizzate

- **Streamlit** – Framework per applicazioni web interattive
- **Pandas** – Libreria per analisi e manipolazione dati
- **NLTK** – Natural Language Toolkit per elaborazione del linguaggio naturale
- **BeautifulSoup4** – Libreria per parsing HTML/XML
- **Requests** – Libreria per richieste HTTP
- **Parsel** – Libreria per estrazione dati con XPath e CSS selectors

---

## 🔧 Funzionalità

L'applicazione offre strumenti per:
- Analisi di contenuti testuali
- Elaborazione di dati strutturati
- Esportazione risultati in formato CSV
- Interfaccia web intuitiva

---

## 📝 Note tecniche

- I modelli NLTK vengono scaricati automaticamente al primo avvio
- L'applicazione supporta l'elaborazione di testi in più lingue
- Tutti i dati vengono processati localmente per garantire la privacy

---

## 🎓 Scopo didattico

Questa applicazione è sviluppata per scopi didattici nell'ambito del corso SMA A.A. 2025-2026 per dimostrare tecniche di analisi dati e sviluppo di applicazioni web con Python.
