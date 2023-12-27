from google.cloud import bigquery
import os
import csv
import pandas as pd
import pandasql as ps
import holidays
import time
import base64
import io
import time
import re

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, State, callback_context
from plotly.subplots import make_subplots
import plotly.graph_objects as go

#                                                       Function Space


# - PARSING DEI FILE
def parse_contents(contents, filename):
    content_type, content_string = contents.split(',')

    decoded = base64.b64decode(content_string)
    try:
        if 'csv' in filename:
            df = pd.read_csv(io.StringIO(decoded.decode('utf-8')), sep=';')
            return df, None  # Restituisce il DataFrame e nessun messaggio di errore
    except Exception as e:
        return None, f'Errore durante il processamento del file {filename}: {e}'

    return None, f'File {filename} non supportato'


# - FORMATTING DATA FROM DRAG AND DROP
def formatting_consumption_data(dataset):
    #####################################################################################################################
    # FORMATTAZIONE FILE CSV
    #####################################################################################################################

    df = dataset
    df = df.transpose()
    df['index'] = df.index

    # Creo lista per aggiustare i valori delle fasce orarie listati in modo errato
    giorno = []

    for item in df['index']:
        giorno.append(item)
    giorno = [s for s in giorno if s != 'Giorno']

    df = df.iloc[:-1, :]  # elimino ultima riga del dataset con valori nulli
    df['Giorno'] = giorno  # ripristino i valori corretti aggiungendo la lista delle fasce orarie
    del df["index"]  # elimino colonna index
    df = df.reset_index(drop=True)  # elimino il vero e proprio index

    # shift column 'Name' to first position
    first_column = df.pop('Giorno')

    # insert column using insert(position,column_name,
    # first_column) function
    df.insert(0, 'Giorno', first_column)

    # setto index = colonna Giorno
    # df = df.set_index('Giorno')
    df = df.transpose()
    df.columns = df.iloc[0]
    df = df[1:]

    # Trasformazione dell'indice in una colonna
    df.reset_index(inplace=True)
    df = df.rename(columns={'index': 'Giorno'})

    #####################################################################################################################
    # ELABORAZIONE DATI DA MATRICE A DATASET
    #####################################################################################################################

    hour = []
    single_hour = []
    giorno_esponenziale = []
    lunghezza_mese = []

    # Creo lista con dettaglio quartorario sulle 96 colonne del dataset iniziale
    for giorno in df['Giorno']:
        for column in df.columns:
            hour.append(column)
            single_hour.append(column[0:2])

    hour = [s for s in hour if s != 'Giorno']
    print(len(hour))
    single_hour = [s for s in single_hour if s != 'Gi']

    # Creo lista che avrà il numero di righe del dataset originario (ovverio i giorni del mese) moltiplicate le 96 colonne

    for giorno in df['Giorno']:
        for num in range(96):
            giorno_esponenziale.append(giorno)

    # Creo lista che avrà i valori di CONSUMO IN KW di ciscun giorno per le 96 colonne (escluso Giorno)
    counter = -1

    df_transposed = df.transpose()
    for days in range(len(df_transposed.columns)):
        counter = counter + 1
        lunghezza_mese.append(counter)

    consumi_kw_h = []

    df_transposed = df_transposed.set_axis(lunghezza_mese, axis='columns')
    df_transposed = df_transposed.iloc[1:, :]

    for num in range(len(df_transposed.columns)):
        for element in df_transposed[num]:
            consumi_kw_h.append(str(element).replace(',', '.'))

    data_frame = pd.DataFrame()
    data_frame['date'] = giorno_esponenziale
    try:
        data_frame['full_hour'] = hour
    except ValueError as e:
        print(e)
        pass

    data_frame['hour'] = single_hour
    data_frame['consumi_kw_h'] = consumi_kw_h

    #####################################################################################################################
    # AGGIUNTA CAMPI MESE/STAGIONE
    #####################################################################################################################

    month, season, fascia_oraria, anno = ([] for i in range(4))
    data_frame['date'] = data_frame['date'].astype(str)

    for item in data_frame['date']:
        if item[3:5] == '01':
            month.append('gen')
            season.append('inverno')
            anno.append(item[6:])

        if item[3:5] == '02':
            month.append('feb')
            season.append('inverno')
            anno.append(item[6:])

        if item[3:5] == '03':
            month.append('mar')
            season.append('primavera')
            anno.append(item[6:])

        if item[3:5] == '04':
            month.append('apr')
            season.append('primavera')
            anno.append(item[6:])

        if item[3:5] == '05':
            month.append('mag')
            season.append('primavera')
            anno.append(item[6:])

        if item[3:5] == '06':
            month.append('giu')
            season.append('estate')
            anno.append(item[6:])

        if item[3:5] == '07':
            month.append('lug')
            season.append('estate')
            anno.append(item[6:])

        if item[3:5] == '08':
            month.append('ago')
            season.append('estate')
            anno.append(item[6:])

        if item[3:5] == '09':
            month.append('set')
            season.append('autunno')
            anno.append(item[6:])

        if item[3:5] == '10':
            month.append('ott')
            season.append('autunno')
            anno.append(item[6:])

        if item[3:5] == '11':
            month.append('nov')
            season.append('autunno')
            anno.append(item[6:])

        if item[3:5] == '12':
            month.append('dic')
            season.append('inverno')
            anno.append(item[6:])

    #####################################################################################################################
    # FORMATTAZIONE DATE
    #####################################################################################################################

    # Formattiamo il campo data nel TimeStamp YYYY-MM-DD
    data = []
    for value in data_frame['date']:
        data.append(value[6:] + '-' + value[3:5] + '-' + value[0:2])

    del data_frame["date"]  # elimino colonna data formattata male
    data_frame['date'] = data  # aggiungo nuova colonna data formattata YYYY-MM-DD
    data_frame = data_frame[['date', 'full_hour', 'hour', 'consumi_kw_h']]

    # Creo una nuova colonna contenente il giorno della settimana rispetto alla data YYYY-MM-DD nel campo date

    day_of_week = []
    day_of_year = []

    for value in data_frame['date']:
        day = pd.Timestamp(
            value)  # uso funziona TIMESTAMP per creare il giorno settimana dalla singola data es:'2020-10-22'
        day_of_week.append(day.day_name())

    data_frame['giorno_sett'] = day_of_week

    #####################################################################################################################
    # RILEVIAMO LE FESTE
    #####################################################################################################################

    # RILEVIAMO LE FESTE!
    # Usiamo la libreria holydays precedentemente importata - https://www.geeksforgeeks.org/python-holidays-library/
    # Otteniamo la festività a partire dalla data, per poi inserire l'eventuale fascia F3 sulle festività trovate
    holydays = []
    it_holidays = holidays.Italy()  # Inseriamo calendario Italiano come riferimento

    for date in data_frame['date']:
        valore_festa = it_holidays.get(date)
        holydays.append(valore_festa)

    data_frame['holydays'] = holydays

    #####################################################################################################################
    # INSERIAMO LE FASCE ORARIE
    #####################################################################################################################

    # Incredibile ma vero... Usiamo l'SQL per stabilire la corretta fascia oraria considerando le due colonne: giorno_sett e hour
    q1 = """SELECT*,
    CASE
      WHEN giorno_sett REGEXP 'Monday|Tuesday|Wednesday|Thursday|Friday'
        AND hour REGEXP '08|09|10|11|12|13|14|15|16|17|18' THEN 'F1'

      WHEN giorno_sett REGEXP 'Monday|Tuesday|Wednesday|Thursday|Friday'
        AND hour REGEXP '07|19|20|21|22' THEN 'F2'

      WHEN giorno_sett REGEXP 'Saturday'
        AND hour REGEXP '07|08|09|10|11|12|13|14|15|16|17|18|19|20|21|22' THEN 'F2'

      WHEN giorno_sett REGEXP 'Monday|Tuesday|Wednesday|Thursday|Friday|Saturday'
        AND hour REGEXP '23|00|01|02|03|04|05|06' THEN 'F3'

      WHEN giorno_sett REGEXP 'Sunday'      THEN 'F3'

      WHEN holydays NOT NULL THEN 'F3'

    END AS 'fascia_oraria'

    FROM data_frame
    """

    df = ps.sqldf(q1, locals())


    final_df = df



    return final_df


# - UPDATE SU GCP
def gcp_update_tab_consumi(df):
    # Imposta il percorso del file JSON di credenziali
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "solar-api-399515-da9f8b1998a8.json"
    print(df.columns)
    # Crea un client BigQuery
    client = bigquery.Client()

    df_formattet = df.where(pd.notnull(df), "None")


    # Converte il DataFrame in una lista di dizionari

    rows_to_insert = df_formattet.to_dict('records')

    # ID della tabella
    table_id = 'solar-api-399515.id_01.consumi_utenti'

    try:
        # Inserisci i dati nella tabella
        errors = client.insert_rows_json(table_id, rows_to_insert)

        # Controlla se ci sono stati errori
        if errors == []:
            print("I record sono stati inseriti con successo.")
        else:
            print(f"Si sono verificati errori durante l'inserimento dei dati: {errors}")
    except bigquery.exceptions.BigQueryError as e:
        print(f"Si è verificato un errore durante l'inserimento dei dati: {e}")
    except Exception as e:
        print(f"Si è verificato un errore generale: {e}")

# Funzione per creare il grafico a ciambella
def donut_chart_trace(df):
    text_size = 20
    hole_size = 0.2
    df['consumi_kw_h'] = pd.to_numeric(df['consumi_kw_h'], errors='coerce')
    consumi_per_fascia = df.groupby('fascia_oraria')['consumi_kw_h'].sum()
    colors = ['yellow', 'gold', 'orange', 'lightyellow']

    trace = go.Pie(
        labels=consumi_per_fascia.index,
        values=consumi_per_fascia,
        hole=hole_size,
        hoverinfo='label+percent',
        textinfo='label+value+percent',
        textfont_size=text_size,
        marker=dict(colors=colors),
        showlegend=False  # Aggiunto per non mostrare la legenda
    )
    return trace

# Funzione per creare il grafico di tendenza
def trendline_chart_trace(df):
    df['date'] = pd.to_datetime(df['date'])
    df['consumi_kw_h'] = pd.to_numeric(df['consumi_kw_h'], errors='coerce')
    consumi_per_giorno = df.groupby('date')['consumi_kw_h'].sum()

    max_consumo = consumi_per_giorno.max()
    colors = [f"rgb(255, {255 - int(255 * (val / max_consumo))}, 0)" for val in consumi_per_giorno]

    traces = []
    for i in range(len(consumi_per_giorno) - 1):
        traces.append(go.Scatter(
            x=consumi_per_giorno.index[i:i + 2],
            y=consumi_per_giorno.values[i:i + 2],
            mode='lines+markers+text',
            line=dict(color=colors[i], width=3),
            marker=dict(size=10),
            text=consumi_per_giorno.values[i:i + 2],
            textposition="top center",
            showlegend=False  # Aggiunto per non mostrare la legenda
        ))
    return traces

# Funzione per creare la heatmap
def heatmap_trace(df):
    df['date'] = pd.to_datetime(df['date'])
    df['consumi_kw_h'] = pd.to_numeric(df['consumi_kw_h'], errors='coerce')
    df['hour'] = df['full_hour'].str[:2].astype(int)
    df['weekday'] = df['date'].dt.day_name()

    heatmap_data = df.pivot_table(index='weekday', columns='hour', values='consumi_kw_h', aggfunc='mean')

    trace = go.Heatmap(
        z=heatmap_data.values,
        x=heatmap_data.columns,
        y=heatmap_data.index,
        colorscale='YlOrRd',
        colorbar=dict(
            len=0.42,  # Lunghezza della colorbar
            lenmode='fraction',  # Lunghezza in termini di frazione dell'altezza totale
            y=0.2,  # Posiziona il centro della colorbar a metà dell'altezza del grafico
            yanchor='middle',  # L'ancoraggio 'middle' allinea il centro della colorbar con y
            title='Consumi kWh'
        )
    )
    return trace

# Funzione per creare report finale
def report_subplot(df):
    # Creazione della figura con subplot
    fig = make_subplots(rows=2, cols=2,
                        specs=[[{"type": "pie"}, {"type": "xy"}],
                               [{"type": "xy", "colspan": 2}, None]],
                        subplot_titles=("Donut Chart", "Trendline Chart", "Heatmap"),
                        vertical_spacing=0.1)  # Riduci lo spazio verticale tra i subplot

    # Aggiunta dei tracciati ai subplot
    donut_trace = donut_chart_trace(df)
    fig.add_trace(donut_trace, row=1, col=1)

    for trace in trendline_chart_trace(df):
        fig.add_trace(trace, row=1, col=2)

    heatmap_tr = heatmap_trace(df)
    fig.add_trace(heatmap_tr, row=2, col=1)

    # Aggiornamento del layout per ridurre i margini
    fig.update_layout(title_text="", height=1000, width=1200,
                      margin=dict(l=10, r=10, t=30, b=10))  # Margini più stretti

    # Opzionale: aggiornamento del padding all'interno di ciascun subplot, se necessario
    # fig.update_traces(row=1, col=1, selector=dict(type='pie'), pad=dict(t=10, b=10))
    # fig.update_traces(row=1, col=2, selector=dict(type='scatter'), pad=dict(t=10, b=10))
    # fig.update_traces(row=2, col=1, selector=dict(type='heatmap'), pad=dict(t=10, b=10))

    # Mostra il grafico

    return fig






# Inizializza l'applicazione Dash con il tema Bootstrap
app = dash.Dash(__name__)
server = app.server


# Definizione degli stili (come da tuo codice)
stile_giallo = {
    'backgroundColor': '#f8bc00',  # Codice colore per il giallo
    'padding': '10px',
    'fontFamily': 'Arial',
    'textAlign': 'center'
}

stile_input = {
    'width': '60%',
    'height': '50px',
    'margin': '5px',
    'borderRadius': '10px',
    'padding': '5px'
}

stile_upload = {
    'width': '40%',
    'height': '50px',
    'lineHeight': '30px',
    'borderWidth': '1px',
    'borderStyle': 'dashed',
    'borderRadius': '5px',
    'textAlign': 'center',
    'margin': '10px auto',
    'display': 'inline-block'
}



form_layout = html.Div([
                        html.Div([

                            dcc.Input(id='id_pod', type='text', placeholder='ID POD', style=stile_input),
                            html.Br(),
                            dcc.Input(id='nome', type='text', placeholder='Nome', style=stile_input),
                            html.Br(),
                            dcc.Input(id='cognome', type='text', placeholder='Cognome', style=stile_input),
                            html.Br(),
                            dcc.Input(id='azienda', type='text', placeholder='Azienda', style=stile_input),
                            html.Br(),
                            dcc.Input(id='ambito', type='text', placeholder='Ambito', style=stile_input),
                            html.Br(),
                            dcc.Input(id='superficie_tetto', type='text', placeholder='Superficie tetto', style=stile_input),
                            html.Br(),
                            dcc.Input(id='dipendenti', type='number', placeholder='Dipendenti', style=stile_input),
                            html.Br(),
                            dcc.Input(id='localita', type='text', placeholder='Località', style=stile_input),
                            html.Br(),
                            dcc.Input(id='distributore', type='text', placeholder='Distributore', style=stile_input),
                            html.Br(),

                            dcc.RadioItems(
                                options=[
                                    {'label': 'Producer', 'value': 'Producer'},
                                    {'label': 'Consumer', 'value': 'Consumer'},
                                    {'label': 'Prosumer', 'value': 'Prosumer'}
                                ],
                                value='Producer',
                                id='role',
                                style={'display': 'flex',
                                       'justifyContent': 'center',
                                       'fontSize': '20px',  # Aumenta la dimensione del testo
                                       'margin': '10px 0'  # Aumenta il margine verticale
                                       },
                                labelStyle={'display': 'inline-block',
                                            'marginRight': '20px',  # Aumenta lo spazio tra le etichette
                                            'cursor': 'pointer'}  # Aggiunge stile al puntatore
                            ),



                            #Drag and Drop
                            dcc.Upload(
                                id='upload-data',
                                children=html.Div(['Trascina o ', html.A('seleziona un file CSV')]),
                                style=stile_upload,
                                multiple=True
                            ),
                            dcc.Loading(html.Div(id='file-name-display', style={
                                'textAlign': 'center',
                                'fontWeight': 'bold',
                                'fontFamily': 'Arial, sans-serif'  # Puoi scegliere il font che preferisci
                            }))

                        ], style=stile_giallo),

                        #Submit button
                        dbc.Button('Submit', id='submit-button', n_clicks=0, className="me-1",
                                   style={'backgroundColor': '#FFFF99',
                                          'color': 'black',
                                          'padding': '15px 40px',  # Aumenta lo spazio interno
                                          'fontSize': '18px',  # Aumenta la dimensione del testo
                                          'lineHeight': '1.5'  # Ajusta l'altezza della linea per il testo
                                          }),

                        dcc.Store(id='store-id-pod'),  # Aggiungi questo per memorizzare id_pod
                        html.Div(id='output-id-pod'),  # Div per visualizzare il valore di id_pod
                        html.Br(),

                        #Output area


                    ], style=stile_giallo)

report_layout = dcc.Graph(id="report_layout")

# Layout dell'applicazione
app.layout = html.Div([
    dcc.Tabs([
        dcc.Tab(label='Tab one', children=[form_layout]),
        dcc.Tab(label='Tab two', children=[dcc.Loading(report_layout)]),
    ])
])



#############################################################




###############################################
# Callback Tabella Iscritti
@app.callback(
    [#Output('output-data-upload', 'value'),
     Output('store-id-pod', 'data')],
    [Input('submit-button', 'n_clicks')],
    [
     State('id_pod', 'value'),
     State('nome', 'value'),
     State('cognome', 'value'),
     State('azienda', 'value'),
     State('ambito', 'value'),
     State('superficie_tetto', 'value'),
     State('dipendenti', 'value'),
     State('localita', 'value'),
     State('distributore', 'value')]
)
def update_tabella_iscritti(n_clicks, id_pod, nome, cognome, azienda, ambito, superficie_tetto,
                            dipendenti, localita, distributore):
    if n_clicks > 0:
        time.sleep(1)  # Simula il tempo di elaborazione

        '''
        output_text = (f'DATI INSERITI:\nID_POD: {id_pod}\nNome: {nome}\nCognome: {cognome}\n'
                       f'Azienda: {azienda}\nAmbito: {ambito}\nSuperficie tetto: {superficie_tetto}\n'
                       f'Dipendenti: {dipendenti}\nLocalità: {localita}\nDistributore: {distributore}')
        '''


    else:
        output_text = "Inserisci i dati del form"

    # Restituisce sia il testo per l'output che i dati da memorizzare in dcc.Store
    return [{'id_pod': id_pod}]







###############################################
#Callback File drag and drop

#Qui prendiamo l'id_pod creato nel form di sopra, e lo alleghiamo come colonna al dataset dei consumi
@app.callback(
    [Output('output-id-pod', 'children'),
     Output('report_layout', 'figure')],  # Specifica che ci sono due output: uno per il testo e uno per la figura
    [Input('submit-button', 'n_clicks')],
    [State('store-id-pod', 'data'),
     State('id_pod', 'value'),
     State('upload-data', 'contents'),
     State('upload-data', 'filename')]
)
def upload_tabella_consumi(n_clicks, stored_id_pod, id_pod, list_of_contents, list_of_names):
    if n_clicks > 0 and stored_id_pod is not None:
        if list_of_contents:
            dataframes = []
            errors = []
            for contents, name in zip(list_of_contents, list_of_names):
                df, error = parse_contents(contents, name)
                if error:
                    errors.append(error)
                else:
                    dataframes.append(df)

            if errors:
                return '\n'.join(errors), dash.no_update  # dash.no_update previene l'aggiornamento del secondo output

            if len(dataframes) > 1 and all(df.columns.equals(dataframes[0].columns) for df in dataframes):
                concatenated_df = pd.concat(dataframes)
                try:
                    formatted_dataset = formatting_consumption_data(concatenated_df)
                    formatted_dataset["id_pod"] = id_pod
                    gcp_update_tab_consumi(formatted_dataset)
                    report = report_subplot(formatted_dataset)
                    return "File caricati e processati correttamente.", report

                except Exception as e:
                    print(f"Si è verificato un errore generale: {e}")
                    return f"Errore: {e}", dash.no_update

            elif len(dataframes) == 1:
                try:
                    formatted_dataset = formatting_consumption_data(dataframes[0])
                    formatted_dataset["id_pod"] = id_pod
                    gcp_update_tab_consumi(formatted_dataset)
                    report = report_subplot(formatted_dataset)
                    return "File caricato e processato correttamente.", report

                except Exception as e:
                    print(f"Si è verificato un errore generale: {e}")
                    return f"Errore: {e}", dash.no_update

            else:
                return "I file caricati hanno strutture diverse.", dash.no_update

        else:
            return "Nessun file selezionato.", dash.no_update

    else:
        return 'ID POD non è stato ancora memorizzato', dash.no_update







###############################################
#Callback visualizza dati caricati

@app.callback(
    Output('file-name-display', 'children'),
    Input('upload-data', 'filename'),
    Input('submit-button', 'n_clicks'),
)
def update_file_name_display(uploaded_filenames, submit_button):
    if uploaded_filenames is not None:
        if isinstance(uploaded_filenames, list):
            # Se sono stati caricati più file, li unisce in una stringa
            time.sleep(1)
            return html.Ul([html.Li(file_name) for file_name in uploaded_filenames])

        else:
            # Se è stato caricato un solo file
            return f"Caricato: {uploaded_filenames}"


    elif uploaded_filenames is None and submit_button:

        time.sleep(1)
        return "Nessun file caricato"


    if uploaded_filenames is not None and submit_button:
        if isinstance(uploaded_filenames, list):
            # Se sono stati caricati più file, li unisce in una stringa
            time.sleep(1)
            return html.Ul([html.Li(file_name) for file_name in uploaded_filenames])

        else:
            # Se è stato caricato un solo file
            return f"Caricato: {uploaded_filenames}"


    return "Nessun file caricato"



if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8000)
