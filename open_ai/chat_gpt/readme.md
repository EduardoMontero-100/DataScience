## VARIABLES
Declaramos dos variables:

filter_date: una variable de tipo DATE que almacenará la fecha de filtrado.
M_ABT: una variable de tipo INT64 que almacenará el valor del mes utilizado en abt_capro.
Asignamos valores a las variables:

filter_date: asignamos el valor de next_execution_date (fecha de ejecución del DAG) utilizando la función ds.
M_ABT: asignamos el valor del mes anterior al de filter_date utilizando la función DATE_ADD para restar un mes y la función FORMAT_DATE para convertirlo en un formato numérico.

## CREACIÓN DE TABLA TEMPORAL
Creamos una tabla temporal llamada tmp_capro_batch_preds en el proyecto y dataset especificados.

La tabla se crea utilizando los resultados de una predicción batch realizada con el modelo capro_bq ubicado en el proyecto y ruta especificados.

La predicción se realiza utilizando los datos de la tabla abt_capro_m del mismo proyecto y dataset, pero solo se seleccionan los registros que tengan el valor de periodo igual a M_ABT.