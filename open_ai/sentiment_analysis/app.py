# Importamos librerias
import time
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from transformers import pipeline
from sklearn.model_selection import train_test_split
from utils import get_completion, read_file, generate_confusion_matrix
from sklearn.metrics import accuracy_score
# RPD : https://platform.openai.com/docs/guides/rate-limits/usage-tiers?context=tier-free
def main():
    
    # Definimos variables
    page_title = 'Sentiment Analysis'
    page_icon = ':cloud'
    layout = 'centered'

    # Configuramos la app
    st.set_page_config(page_title=page_title, page_icon= page_icon, layout= layout)
    st.title(page_title+ " "+ page_icon)

    st.header(f'Sube el archivo con las encuestas')
    uploaded_file = st.file_uploader("Selecciona archivo CSV", type = ["csv"])
    
    if uploaded_file is not None:
        st.success("Archivo subido exitosamente!")
        
        # Convertimos el archivo a pandas
        df = read_file(uploaded_file)

        # nos quedamos con los campos que necesitamos
        columns = ['Q1_NPS_GROUP', 'Q1', 'Q2', 'Q3', 'TECNOLOGIA', 'ID_SUSCRIPCION', 'RecordedDate', 'DNI']
        df = df[columns]

        # excluimos las dos primeras filas
        df = df.iloc[2: df.shape[0]]

        # renombramos columnas
        df.rename(columns = {
                'Q1_NPS_GROUP': 'target',
                'Q1': 'target_num',
                'Q2': 'motivo',
                'Q3': 'factor',
                'TECNOLOGIA': 'tecnologia',
                'ID_SUSCRIPCION': 'id_suscripcion',
                'RecordedDate': 'fecha',
                'DNI': 'id_cliente'
        }, inplace = True)

        # Nos quedamos con los casos que son string
        df = df[df['motivo'].apply(lambda x :isinstance(x, str) )]

        # Nos quedamos con HFC
        df = df[df['tecnologia']== 'HFC']

        # Nos quedamos solo con promotores y detractores
        df = df[(df['target']== 'Detractor') | (df['target']== 'Promotor')]

        # Dropeamos los NAs
        df.dropna(inplace = True)

        # Balanceamos la clase
        df_promotor = df[df['target'] == 'Promotor']
        n_promotor = df_promotor.shape[0]

        df_detractor = df[df['target'] == 'Detractor']
        df_detractor = df_detractor.sample(n = n_promotor)

        df = pd.concat([df_promotor, df_detractor])
        df = df.sample(frac = 1)

        # Separamos en X e y
        X = df['motivo'].astype(str)
        y = df['target'].astype(str)

        # Separamos en entrenamiento (ejemplos) prueba (querys)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.01)

        # Reseteamos los indices
        X_train = X_train.reset_index()['motivo']
        X_test = X_test.reset_index()['motivo']
        y_test = y_test.reset_index().drop('index', axis =1)['target']

        # Definimos la barra de proceso
        st.warning(f'Aplicamos el modelo sobre un total de {len(X_test)} encuestas')

        progress_bar = st.progress(0)
        porc_avance = 100/len(X_test)
        
        # Listas para acumular resultados y errores
        y_pred = []
        y_score = []
        
        # Cargar el pipeline de análisis de sentimientos en español
        model = 'nlptown/bert-base-multilingual-uncased-sentiment'
        sentiment_analyzer_es = pipeline("sentiment-analysis", model=model)
        
        for i in range(0, len(X_test)):
            
            # Realizar el análisis de sentimientos
            resultado = sentiment_analyzer_es(X_test.iloc[i])
            
            sentimiento = 'Promotor' if resultado[0]['label'] == '5 stars' or resultado[0]['label'] == '4 stars' or resultado[0]['label'] == '3 stars' else 'Detractor'
            score = resultado[0]['score']
            
            y_pred.append(sentimiento)
            y_score.append(score)
            
            progress_bar.progress(int((i+1)*porc_avance))
            
        y_pred = pd.Series(y_pred)
        y_score = pd.Series(y_score)
        
        # Generamos matriz de confusión
        generate_confusion_matrix(y_test, y_pred)
        
        # Calculamos métricas de performance
        y_test = y_test.apply(lambda x: 1 if x == 'Promotor' else 0)
        y_pred = y_pred.apply(lambda x: 1 if x == 'Promotor' else 0)

        accuracy = round(accuracy_score(y_test, y_pred), 2) *100
        #recall = round(recall_score(y_test, y_pred),2) *100
        #precision = round(precision_score(y_test, y_pred),2) *100

        st.success(f'Accuracy: {accuracy} %')
        #st.success(f'Recall: {recall} %')
        #st.success(f'Precision: {precision} %')
        
        
    
if __name__ == "__main__":
    main()