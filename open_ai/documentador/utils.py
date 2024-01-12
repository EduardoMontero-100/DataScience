# Definición de función para llamar al modelo gpt-3.5-turbo
def get_completion(prompt, model = "gpt-3.5-turbo"):
    """
    Función que llama al modelo gpt-3.5-turbo y recibe un prompt de necesidad 
    de documentación 
    """
    import openai
    # Seteamos la secret key que se obtiene desde la cuenta registrada en openai
    #openai.api_key = "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"

    messages = [{"role": "user", "content": prompt}]
    response = openai.ChatCompletion.create(
        model = model,
        messages = messages,
        temperature = 0
    )
    
    return response.choices[0].message['content']

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
