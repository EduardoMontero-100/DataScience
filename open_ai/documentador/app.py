# Importamos librerias
import streamlit as st
import time
from utils import get_completion, code_split

def main():
    
    # Definimos variables
    page_title = 'Documentador de Código'
    page_icon = ':books'
    layout = 'centered'
    # Definimos decorador genérico
    question_flow = "Por favor, genera un diagrama de flujo o en su defecto un gráfico que esquematice el siguiente código"
    decorator_doc = "Escribe el output en formato markdown"
    decorator_flow = decorator_doc
    # Declaramos el error de tamaño de texto de entrada
    text_error = "This model's maximum context length is 4097 tokens"
    # Declaramos la variables que captura errores
    exception_string = ''
    # Declaramos la variable de cantidad de líneas máximas de código para documentar
    lines = 1500
    # Nombre del modelo 
    model_name = 'gpt-3.5-turbo'

    
    
    # Definimos el prompt template
    prompt_template = """ 
        {question}
        {code}
        {decorator}
    """
    
    # Configuramos la app
    st.set_page_config(page_title=page_title, page_icon= page_icon, layout= layout)
    st.title(page_title+ " "+ page_icon)

    st.header(f'Sube el archivo a documentar')
    uploaded_file = st.file_uploader("Selecciona archivo")
    
    if uploaded_file is not None:
        st.success("Archivo subido exitosamente!")
        
        # Leemos el archivo
        content = uploaded_file.read()
        
        # Definimos la pregunta del prompt para la documentación en base al nombre del archivo
        filename = uploaded_file.name
        if filename.endswith('.py'): # Aplica principalmente para dag files
            question_doc = "Por favor, escribe documentación técnica para este código.\nHazlo fácil para que un desarrollador que no sabe Python lo pueda entender,\nhazlo en menos de 50 líneas. Sigue el formato que se encuentra en este link https://tecocloud.atlassian.net/wiki/spaces/AAYBD/pages/3248653416/GCP+stgPerfilesFija."
            question_flow = "Por favor, genera un diagrama de flujo para el siguiente código"
            decorator_flow = "Escribe el output en formato markdown de forma tal que se puedan apreciar los nombres de los elementos y sus relaciones"

        elif 'prediction_abt' in filename: # Aplica para archivo de predicion.sql
            question_doc = "Por favor, escribe documentación técnica para este código.\nHazlo en menos de 50 líneas\nHazlo simple para que cualquier desarrollador lo pueda entender.\nDetalla cada uno de los pasos."
            
        elif 'insert_score_abt' in filename: # Aplica para el archivo de inserción de abt
            question_doc = "Por favor, escribe documentación técnica para este código.\nHazlo en menos de 50 líneas\nDetalla cada uno de los pasos."
            
        elif 'load_abt' in filename: # Aplica para el archivo load_abt
            question_doc = "Por favor, escribe documentación técnica para este código.\nHazlo simple para que cualquier desarrollador lo entienda.\nSi encuentas operaciones de CREATE, INSERT Y UPDATE resume los campos usados, no escribas cada uno de ellos."
            
        elif 'create' in filename: # Aplica para archivos de creación de tablas    
            question_doc = "Por favor, escribe documentación técnica para este código.\nHazlo simple para que cualquier desarrollador lo entienda.\nDetalla cada unos de los campos."
        
        else: # Para el resto de casos usamos algo más genérico
            question_doc="""Por favor, escribe documentación técnica para este código.\nHazlo en menos de 50 líneas,\nhazlo simple para que cualquier desarrollador lo entienda,\nindica el nombre de las tablas que intervienen en el proceso:"""      
  
        ## Controlamos errores
        # Definimos prompt para la documentación
        prompt_doc = prompt_template.format(question=question_doc,
                                            code = content,
                                            decorator=decorator_doc)
        
        st.warning("Ahora haremos un control de errores. !Permite que el modelo piense!")
        try:
            get_completion(prompt_doc)
        except Exception as e:
            st.warning(f"An error occurred while trying to executing {model_name}: {e}")
            exception_string = str(e)    
  
        if text_error in exception_string.strip() or len(content.splitlines()) > lines:
            st.warning(f"""La cantidad de lineas de código es demasiado grande para {model_name}, tiene {len(content.splitlines())} líneas.
                           Se recomienda dividir el código en varios procesos/archivos""")
        else:
            
            if 'create' in filename and len(content.splitlines())>100:
                st.warning(f"""El modelo {model_name} tiene una limitación de respecto a la cantidad de tokens de salida.
                           Se recomienda armar archivos de creación de 50 campos y después unificar la documentación generada""")
            else:
                
                ## Definimos prompt para la documentación
                prompt_doc = prompt_template.format(question=question_doc,
                                                    code = content,
                                                    decorator=decorator_doc)
                
                # Definimos prompt para el diagrama de flujo.
                prompt_flow = prompt_template.format(question=question_flow,
                                                    code = content,
                                                    decorator=decorator_flow)
                # Juntamos los prompts en un diccionario
                prompts = {'documentacion':prompt_doc, 'diagrama':prompt_flow}
                responses = {}
            
                ## Obtenemos la documentación y el diagrama de flujo
                start = time.time()
                i = 0
                st.warning('Procesando documentación')
                st.warning('Primero generamos la documentación y después el diagrama de flujo')
                progress_bar = st.progress(0)
                
                for key, value in prompts.items():
                    # Llamamos a la función que ejecuta el prompt y aplica la predicción
                    responses[key] = get_completion(value)
                    i = i +50
                    progress_bar.progress(i)
                
                end = time.time()    
                elapsed_time = end - start
                st.warning(f"El proceso tomó {int(elapsed_time)} segundos")
                
                st.success('Documentación')
                st.write(responses['documentacion'])
                st.success('Diagrama')
                st.write(responses['diagrama'])


if __name__ == "__main__":
    main()