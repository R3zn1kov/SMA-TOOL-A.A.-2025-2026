# Test Directory

Questa directory contiene script provvisori per testare e sviluppare nuove funzionalità per l'applicazione di estrazione di contenuti testuali.

## Utilizzo

- Deposita qui script temporanei per testare nuove funzionalità
- Usa questa directory per sperimentare con miglioramenti prima di integrarli nel codice principale
- I file in questa directory sono per scopi di sviluppo e testing

## Struttura consigliata

```
test/
├── README.md              # Questo file
├── .gitkeep              # Mantiene la directory nel repository
├── reddit_experiments.py # Script per testare funzionalità Reddit
├── news_experiments.py   # Script per testare funzionalità Google News
├── ui_prototypes.py      # Prototipi di interfaccia utente
└── data_analysis.py      # Script per analisi dati
```

## Import delle classi principali

Per utilizzare le classi di estrazione nei tuoi script di test:

```python
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools import RedditExtractor, GoogleNewsExtractor

# Inizializza gli estrattori
reddit_extractor = RedditExtractor()
google_news_extractor = GoogleNewsExtractor()

# Testa le funzionalità...
```

## Note

- I file in questa directory non dovrebbero essere inclusi nelle release di produzione
- Assicurati di testare le funzionalità prima di integrarle nel codice principale
- Documenta eventuali dipendenze aggiuntive necessarie per i tuoi esperimenti