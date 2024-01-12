# Definición de función para llamar al modelo gpt-3.5-turbo; gpt-3.5-turbo-1106; text-davinci-003
# https://platform.openai.com/docs/models/gpt-3-5
def get_completion(prompt, model = "text-davinci-003"):
    """
    Función que llama al modelo gpt-3.5-turbo y recibe un prompt de necesidad 
    de documentación 
    """
    import openai
    # Seteamos la secret key que se obtiene desde la cuenta registrada en openai
    #openai.api_key = "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"

    response = openai.Completion.create(
        engine = model,
        prompt = prompt,
        temperature = 0
    )
    
    return response.choices[0].text.strip()

# Definimos función de spliteo
def code_split(content, field_amount = 50):
    """ Función que separa el código en varios chunks para poder documentar códigos extensos.
    
    inputs:
    ------
    - content: string que contiene el texto
    - field_amount: tamaño de los bloque de código. Por defecto 50 
    
    outputs:
    -------
    - code_list: lista que contiene el código spliteado
    """
    
    # importamos librerias
    import math
    # declaramos variables
    code_list = []
    init_amount = 0
    
    # posición de inicio
    begin_pos = content.find('(')
    # posición de fin
    end_pos = content.find(')')

    # capturamos los campos 
    fields = content[begin_pos+1:end_pos]
    fields_list = fields.splitlines()
    # capturamos el inicio del codigo
    code_init = content[:begin_pos+1]
    # capturamos el final del codigo
    code_end = content[end_pos+1:]
    # capturamos la cantidad de grupos
    fields_chunks = math.ceil(len(fields_list)/field_amount)

    for i in range(1,fields_chunks):

        code = code_init + ''.join(fields_list[init_amount:field_amount*i]) + code_end
        init_amount = field_amount*i
        code_list.append(code)
        
    return code_list

# Función para leer y convertir un archivo a dataframe de pandas
def read_file(file):
    """Función para leer y convertir un archivo a dataframe de pandas

    inputs:
    -------
        file (file): Archivo plano

    outputs:
    -------
        df (dataframe): Archivo transformado a dataframe
    """
    import pandas as pd
    import streamlit as st
    
    if file is not None:
        try:
            # Use Pandas to read the file into a DataFrame
            df = pd.read_csv(file)
            return df
        except Exception as e:
            st.error(f"Error: {e}")
    return None

# Función para generar la matriz de confusión
def generate_confusion_matrix(y_true, y_pred):
    """ 
    Función para generar la matriz de confusión
    
    inputs:
    ------
        y_true: lista labels verdaderas separadas por coma
        y_pred: lista de lables predichas separadas por coma    
        
    outputs:
        muestra la matriz de confusión
    """
    import numpy as np
    import seaborn as sns
    import streamlit as st
    import matplotlib.pyplot as plt
    from sklearn.metrics import confusion_matrix
    # Compute confusion matrix
    cm = confusion_matrix(y_true, y_pred)

    # Plot confusion matrix using seaborn heatmap
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", xticklabels=np.unique(y_true), yticklabels=np.unique(y_true))
    plt.xlabel("Predicted")
    plt.ylabel("True")
    
    st.set_option('deprecation.showPyplotGlobalUse', False)
    st.pyplot()
    
def sentiment_analyzer_es(texto, model = 'nlptown/bert-base-multilingual-uncased-sentiment'):
    """Función que permite hacer un análisis de sentimientos en español 
    
    inputs:
    ------
        - texto (str): Es el texto al que vamos a identificar el sentimiento
    
    outputs:
    -------
        - resultado (tupla): Una tupla con el sentimiento y el score
    """
    
    from transformers import pipeline
    
    # Cargar el pipeline de análisis de sentimientos en español
    sentiment_analyzer_es = pipeline("sentiment-analysis", model=model)
    
    # Realizar el análisis de sentimientos
    resultado = sentiment_analyzer_es(texto)
    
    sentimiento = 'positivo' if resultado[0]['label'] == '5 stars' or resultado[0]['label'] == '4 stars' else 'negativo'
    score = resultado[0]['score']
    
    return sentimiento, score
