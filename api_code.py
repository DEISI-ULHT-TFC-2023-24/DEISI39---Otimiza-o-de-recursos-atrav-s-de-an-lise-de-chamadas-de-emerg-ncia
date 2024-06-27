from flask import Flask, request, jsonify
import pandas as pd
from flask_cors import CORS

app = Flask(__name__)
CORS(app) 

def determina_turno(hora):
    if pd.Timestamp("00:00:00").time() <= hora <= pd.Timestamp("07:30:00").time():
        return "Noite"
    elif pd.Timestamp("08:00:00").time() <= hora <= pd.Timestamp("15:30:00").time():
        return "Dia"
    else:
        return "Tarde"

# Assign hour range based on Turno
def aplica_intervalo_temporal(turno):
    if turno == "Noite":
        return "00:00 - 08:00"
    elif turno == "Dia":
        return "08:00 - 16:00"
    else:
        return "16:00 - 00:00"

@app.route('/recolhe_dados', methods=['GET'])
def recolhe_dados():
    df = pd.read_excel('predsHorarias.xlsx')
    
    # Recolhe os parametros do pedido
    group_by = request.args.get('group_by')
    date_range = request.args.get('date_range')
    date_start, date_end = date_range.replace("%20", "").replace("%2F","/").split('-')
    date_start = date_start.strip()
    date_end = date_end.strip()
    date_start = pd.to_datetime(date_start, format='%d/%m/%Y', dayfirst=True)
    date_end = pd.to_datetime(date_end, format='%d/%m/%Y', dayfirst=True)
    
    df['Dia'] = pd.to_datetime(df['Dia'], format='%d/%m/%Y')
    df = df[(df['Dia'] >= date_start) & (df['Dia'] <= date_end)]
    
    # Verifica qual é o tipo de pedido e agrupa os dados da forma desejada
    if group_by == "diários":
        df_grouped = df.groupby(df['Dia']).agg({'Valor Previsto': 'sum'}).reset_index()
    elif group_by == "turnos":
        df['Hora'] = pd.to_datetime(df['Hora']).dt.time

        df['Turno'] = df['Hora'].apply(determina_turno)
        df['Hora'] = df['Turno'].apply(aplica_intervalo_temporal)

        df_grouped = df.groupby(['Dia', 'Turno', 'Hora']).agg({'Valor Previsto': 'sum'}).reset_index()

        turno_order = pd.Categorical(df_grouped['Turno'], categories=["Noite", "Dia", "Tarde"], ordered=True)
        df_grouped['Turno'] = turno_order

        df_grouped = df_grouped.sort_values(by=['Dia', 'Turno']).reset_index(drop=True)
    elif group_by == "mensais":
        df_grouped = df.groupby(df['Dia'].dt.to_period('M')).agg({'Valor Previsto': 'sum'}).reset_index()
        df_grouped['Dia'] = df_grouped['Dia'].astype(str)
    elif group_by == "anuais":
        df_grouped = df.groupby(df['Dia'].dt.to_period('Y')).agg({'Valor Previsto': 'sum'}).reset_index()
        df_grouped['Dia'] = df_grouped['Dia'].astype(str)
    else:
        df_grouped = df
    
    # Converte todos os dados num dicionário
    data = df_grouped.to_dict(orient='records')
    
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=1440)
