
# Dashboard di Analisi dei Consumi Energetici

Questa dashboard, realizzata con Dash e Plotly, permette di analizzare e visualizzare i consumi energetici. L'applicazione offre funzionalità di caricamento dati, elaborazione e visualizzazione grafica dei consumi.

## Funzionalità Principali

### Caricamento e Analisi dei Dati
- **Parsing dei File CSV**: Supporto per il caricamento di file CSV contenenti dati sui consumi energetici.
- **Elaborazione Dati**: Formattazione e preparazione dei dati per l'analisi.

### Visualizzazione dei Dati
- **Grafici Interattivi**: Utilizzo di grafici Plotly per una rappresentazione dettagliata dei consumi.
  - Grafico a ciambella per le fasce orarie.
  - Grafico di tendenza per i consumi giornalieri.
  - Heatmap per l'analisi settimanale.
- **Report Dinamici**: Creazione di report interattivi basati sui dati caricati.

### Interazione Utente
- **Interfaccia Utente Intuitiva**: Interfaccia facile da usare con componenti di input per il caricamento dei dati.
- **Feedback in Tempo Reale**: Risposte immediate all'interazione dell'utente.

## Tecnologie Utilizzate
- [Dash](https://dash.plotly.com/): Framework per la creazione di applicazioni web in Python.
- [Plotly](https://plotly.com/): Libreria per la creazione di grafici interattivi.
- [Pandas](https://pandas.pydata.org/): Libreria per la manipolazione e l'analisi dei dati.
- [BigQuery](https://cloud.google.com/bigquery): Servizio di data warehouse per l'analisi dei grandi dataset.

## Installazione e Avvio

Per eseguire la dashboard:

1. Clonare il repository.
2. Installare le dipendenze utilizzando `pip install -r requirements.txt`.
3. Avviare l'applicazione con `python app.py`.

## Contribuire

Ogni contributo alla dashboard è benvenuto. Sentiti libero di forkare il progetto, aprire issue e inviare pull request.

## Licenza

Questo progetto è distribuito sotto la licenza MIT.
