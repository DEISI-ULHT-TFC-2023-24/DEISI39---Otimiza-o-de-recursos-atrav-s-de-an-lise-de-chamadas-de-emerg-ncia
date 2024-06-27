import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import datetime
from sklearn.ensemble import RandomForestRegressor
from datetime import timedelta
from sklearn.linear_model import LinearRegression

import pandas as pd

class FileModifiedHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path.endswith("data_chamadas.csv"):
            print("File data_chamadas.csv has been modified!")
            time.sleep(20)
            get_indices(event.src_path)
            # Run your specific code here for file modification
            # For example:
            # process_file(event.src_path)

# Adicionar à tabela o turno a que cada meia hora pertence
def categorize_time(row):
    if datetime.datetime.strptime('00:00:00.000000', '%H:%M:%S.%f') <= row <= datetime.datetime.strptime('07:30:00.000000', '%H:%M:%S.%f'):
        return '1'
    elif datetime.datetime.strptime('08:00:00.000000', '%H:%M:%S.%f') <= row <= datetime.datetime.strptime('15:30:00.000000', '%H:%M:%S.%f'):
        return '2'
    elif datetime.datetime.strptime('16:00:00.000000', '%H:%M:%S.%f') <= row <= datetime.datetime.strptime('23:30:00.000000', '%H:%M:%S.%f'):
        return '3'
    else:
        return 'Outro'

# Adicionar à tabela o valor médio das últimas 4 semanas, para a mesma hora, para o mesmo dia da semana
def addHoraDiaSemana(df, weekday, hora_inicio):
    # filtrar pelo mesmo weekday e horainicio
    filtered_df = df[(df['Weekday'] == weekday) & (df['HoraInicio'] == hora_inicio)]

    last_4_entries = filtered_df.tail(4)

    value = last_4_entries['ACDCalls'].mean()

    value_string = str(value)

    if "[" in value_string and "]" in value_string:

        cleaned_string = value_string.strip("[]")

        try:

            parsed_float = round(float(cleaned_string), 2)

            return parsed_float
        except ValueError:
            print("erro")
    else:

        try:

            parsed_float = round(float(value_string), 2)

            return parsed_float
        except ValueError:
            print("The string is not a valid float.")

# Adicionar à tabela, valores dos ultimos 4 anos para a mesma hora, o mesmo dia
def addHoraDiaAno(df, data, day, month, hora_inicio):
    df['Data'] = pd.to_datetime(df['Data'])
    df['Day'] = df['Data'].dt.day
    df['Month'] = df['Data'].dt.month

    inputDay = int(day)
    inputMonth = int(month)

    # filtrar pelo mesmo dia mes e hora inicio para retirar depois anos diferentes
    filtered_df = df[(df['Day'] == inputDay) & (df['Month'] == inputMonth) & (df['HoraInicio'] == hora_inicio)]

    # escolher ultimos 4 valores
    last_4_entries = filtered_df.tail(4)

    # media de acd
    result = last_4_entries['ACDCalls'].mean()

    df.drop(columns=['Day', 'Month'], inplace=True)

    return result

# Função que dada uma hora, avalia a qual turno pertence
def getTurno(hora):
    hour_dt = pd.to_datetime(hora, format='%H:%M:%S.%f')

    if pd.to_datetime('00:00:00.0000000', format='%H:%M:%S.%f') <= hour_dt <= pd.to_datetime('07:30:00.0000000', format='%H:%M:%S.%f'):
        return '1'
    elif pd.to_datetime('08:00:00.0000000', format='%H:%M:%S.%f') <= hour_dt <= pd.to_datetime('15:30:00.0000000', format='%H:%M:%S.%f'):
        return '2'
    elif pd.to_datetime('16:00:00.0000000', format='%H:%M:%S.%f') <= hour_dt <= pd.to_datetime('23:30:00.0000000', format='%H:%M:%S.%f'):
        return '3'
    else:
        return 'Outro'

# Função que dada uma data, avalia a qual dia da semana pertence
def getDiaSemana(data):
    weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    weekday_index = data.weekday()
    return weekdays[weekday_index]


# Alguns valores previstos vieram em listas então convertemos todos para strings e retiramos os [] se necessário
def process_value(value):
    value_string = str(value)

    if "[" in value_string and "]" in value_string:

        cleaned_string = value_string.strip("[]")

        try:
            return float(cleaned_string)
        except ValueError:
            return None
    else:
        try:
            return float(value_string)
        except ValueError:
            return None



###############################################################
####              Função para fazer previsões              ####
###############################################################

def makePreds(df, hora, data):
    X = df[['indice_hora', 'Media-hora/diaSemana', 'Media_dia/hora']]
    y = df['ACDCalls']

    dia = data.day
    mes = data.month
    turno = getTurno(hora)
    diaSemana = getDiaSemana(data)

    mean_diaAno = addHoraDiaAno(df, data, dia, mes, hora)
    mean_diaSemana = addHoraDiaSemana(df, diaSemana, hora)

    model_LR = LinearRegression()

    model_LR.fit(X, y)
    ultimo_indice = df['indice_hora'].max()

    print(df.isna().sum())

    previsao_hora = model_LR.predict([[ultimo_indice + 1, mean_diaSemana, mean_diaAno]])

    nova_linha = pd.DataFrame(
        {'HoraInicio': [hora], 'Data': [data], 'ACDCalls': [previsao_hora], 'indice_hora': [ultimo_indice + 1],
         'Turno': [turno], 'Weekday': [diaSemana], 'Media-hora/diaSemana': [mean_diaSemana],
         'Media_dia/hora': [mean_diaAno]})
    df = pd.concat([df, nova_linha], ignore_index=True)

    return df


def create_lag_df(df, lag, cols):
    lagged_dfs = []

    # Keep the "indice_hora" column
    lagged_dfs.append(df["HoraInicio"])
    lagged_dfs.append(df["Data"])
    lagged_dfs.append(df["indice_hora"])
    lagged_dfs.append(df["ACDCalls"])

    for col in cols:
        lagged_cols = []
        for n in range(1, lag + 1):
            lagged_col = f"{col}-{n}"
            df_shifted = df[col].shift(n)
            lagged_cols.append(df_shifted)
        # Enumerate lagged columns from 1 to max
        lagged_cols = [lagged_cols[i].rename(f"{col}-{i+1}") for i in range(lag)]
        lagged_dfs.extend(lagged_cols)

    return pd.concat(lagged_dfs, axis=1)

def watch_directory():
    event_handler = FileModifiedHandler()
    observer = Observer()
    observer.schedule(event_handler, path='/home/inem', recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


date_formats = ["%d/%m/%Y", "%Y/%m/%d", "%Y-%m-%d", "%d-%m-%Y"]

# Function to parse dates
def parse_date(date_string):
    for date_format in date_formats:
        try:
        
            return datetime.datetime.strptime(date_string, date_format)
        except ValueError:
            pass
    return None  # If none of the formats succeed

def get_indices(filepath):
    df = pd.read_csv('/home/inem/data_chamadas.csv', delimiter=';')

    #df['Data'] = pd.to_datetime(df['Data'], format='%d-%m-%Y')

    df['Data'] = df['Data'].apply(parse_date)

    # Ordenar o DataFrame por 'Data' e 'HoraInicio'

    df_sorted = df.sort_values(by=['Data', 'HoraInicio'])

    df_filtered = df_sorted[df_sorted['SplitSkill'] == 'CODU 112 NACIONAL']

    # Remover as colunas desnecessarias
    columns_to_drop = ['Id', 'SplitSkill', 'HoraFim', 'AvgSpeedAnswer', 'AvgAbanTime', 'AvgACDTime', 'AcgACWTime',
                       'AbanCalls', 'MaxDelay', 'PercentageAnsCalls', 'FlowIn', 'FlowOut', 'ExtnOutCalls',
                       'AvgExtnOutTime', 'DequeuedCalls', 'AvgTimeToDequeue', 'PercentageACDTime', 'AvgPosStaff',
                       'CallsPerPos']

    df_filtered.drop(columns=columns_to_drop, inplace=True)

    start_date = pd.to_datetime('2020-01-01')
    end_date = pd.to_datetime('2021-12-31')

    # retirar 2020 e 2021 do banco de dados
    df = df_filtered[(df_filtered['Data'] < start_date) | (df_filtered['Data'] > end_date)]

    num_rows = len(df)

    df['indice_hora'] = range(1, num_rows + 1)

    df['Data'] = pd.to_datetime(df['Data'])

    work_dataset(df)


def work_dataset(df):

    '''
    start_date = pd.to_datetime('2023-03-01')
    end_date = pd.to_datetime('2023-03-31')

    # retirar marco para o prever
    df = df[(df['Data'] < start_date) | (df['Data'] > end_date)]
    '''

    df['hora_temp'] = pd.to_datetime(df['HoraInicio'], format='%H:%M:%S.%f')

    df['Turno'] = df['hora_temp'].apply(categorize_time)

    df.drop(columns='hora_temp', inplace=True)

    df['Weekday'] = df['Data'].dt.day_name()
    n_chamadas_hora = df.sort_values(by=['Data', 'HoraInicio'])

    df['Media-hora/diaSemana'] = df.groupby(['Weekday', 'HoraInicio'])['ACDCalls'].transform(
        lambda x: x.rolling(4, min_periods=1).mean())

    df['Media_dia/hora'] = df.groupby('HoraInicio')['ACDCalls'].transform(
        lambda x: x.shift(1).rolling(window=365 * 4, min_periods=1).mean())

    df['Media_dia/hora'] = df['Media_dia/hora'].fillna(df['ACDCalls'])

    run_algorith(df)


def run_algorith(df):

    # Criar lista de horas
    start_time = datetime.datetime.strptime("00:00:00", "%H:%M:%S")

    listaHoras = []

    num_intervals = 48  # 1 para cada meia hora

    for i in range(num_intervals):
        time_str = (start_time + timedelta(minutes=30 * i)).strftime("%H:%M:%S")[:-3]
        listaHoras.append(time_str + ":00.0000000")

    date = df['Data'].iloc[-1].date() + timedelta(days=1)
    startDate = date

    while date < startDate + timedelta(days=31):

        for hora in listaHoras:
            df = makePreds(df, hora, date)

        date += timedelta(days=1)

    n_chamadas_meia_hora = df

    n_chamadas_meia_hora['ACDCalls'] = n_chamadas_meia_hora['ACDCalls'].apply(process_value)

    apply_lags(n_chamadas_meia_hora)


def apply_lags(n_chamadas_meia_hora):
    lags_meiaHora = create_lag_df(n_chamadas_meia_hora, 250, ['ACDCalls'])

    columns_to_drop = ['Media-hora/diaSemana', 'Media_dia/hora', 'Weekday']
    lags_meiaHora = lags_meiaHora.drop(columns=columns_to_drop, errors='ignore')

    lags_meiaHora = lags_meiaHora.dropna()

    # Retiramos o valor de dois entradas por hora, a cada dia, por 31 dias(um mês)
    last_value_minus_1488 = lags_meiaHora['indice_hora'].iloc[-1] - 1488

    lags_meiaHora['Data'] = pd.to_datetime(lags_meiaHora['Data'])
    last_date = lags_meiaHora['Data'].iloc[-1488]

    lags_meiaHora_train = lags_meiaHora.query(f"indice_hora <= {last_value_minus_1488}")
    lags_meiaHora_test = lags_meiaHora.query(f"indice_hora > {last_value_minus_1488}")

    lags_meiaHora_train = lags_meiaHora_train[250:].drop(columns=["indice_hora", "HoraInicio", "Data"])

    lags_meiaHora_train_X = lags_meiaHora_train.drop(columns=["ACDCalls"])
    lags_meiaHora_train_y = lags_meiaHora_train["ACDCalls"]

    n_chamadas_model = RandomForestRegressor(random_state=42)
    n_chamadas_model.fit(lags_meiaHora_train_X, lags_meiaHora_train_y);

    preds = n_chamadas_model.predict(
        lags_meiaHora_test.drop(columns=["indice_hora", "ACDCalls", "HoraInicio", "Data"]))

    export_xlsx(preds, last_date)

def export_xlsx(preds, last_date):
    data_H = []
    hora_H = []
    valor_previsto_H = []

    dia = 1
    iteracao = 0
    itPreds = 0

    print(last_date)
    print(type(last_date))

    currentDate = last_date
    currentHour = datetime.datetime.strptime('00:00:00', '%H:%M:%S')

    while dia < 31:

        if itPreds == len(preds):
            break

        horaIteracao = 1

        while horaIteracao <= 48:

            data = currentDate.date()

            hora = currentHour.strftime("%H:%M:%S")

            valorPrev = int(preds[itPreds])

            print(f"dia {data}, hora {hora}, previsao {valorPrev}")

            data_H.append(data)
            hora_H.append(hora)
            valor_previsto_H.append(valorPrev)

            horaIteracao += 1
            iteracao += 1
            itPreds += 1

            currentHour = currentHour + timedelta(minutes=30)

        currentDate = currentDate + timedelta(days=1)

    tabela_Hora = pd.DataFrame({
        'Dia': data_H,
        'Hora': hora_H,
        'Valor Previsto': valor_previsto_H
    })

    excel_file_path = '/home/inem/predsHorarias.xlsx'

    tabela_Hora.to_excel(excel_file_path, index=False)

    print("Excel exported successfully")

if __name__ == "__main__":
    watch_directory()