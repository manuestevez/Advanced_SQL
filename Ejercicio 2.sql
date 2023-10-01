/*
2. CREAR TABLA DE ivr_summary
Con la base de la tabla ivr_detail vamos a crear la tabla ivr_sumary. Ésta será un resumen de la llamada donde se incluyen los indicadores más importantes de la llamada. Por tanto, sólo tendrá un registro por llamada.
Queremos que tengan los siguientes campos:
ivr_id: identificador de la llamada (viene de detail). phone_number: número llamante (viene de detail). ivr_result: resultado de la llamada (viene de detail). vdn_aggregation: es una generalización del campo vdn_label. Si vdn_label empieza por ATC pondremos FRONT, si empieza por TECH pondremos TECH si es ABSORPTION dejaremos ABSORPTION y si no es ninguna de las anteriores pondremos RESTO. start_date: fecha inicio de la llamada (viene de detail). end_date: fecha fin de la llamada (viene de detail). total_duration: duración de la llamada (viene de detail). customer_segment: segmento del cliente (viene de detail). ivr_language: idioma de la IVR (viene de detail). steps_module: número de módulos por los que pasa la llamada (viene de detail). module_aggregation: lista de módulos por los que pasa la llamada (viene de detail. document_type: en ocasiones es posible identificar al cliente en alguno de los pasos de detail usar el campo con el mismo nombre en detail. document_identification: en ocasiones es posible identificar al cliente en alguno de los pasos de detail usar el campo con el mismo nombre en detail. customer_phone: en ocasiones es posible identificar al cliente en alguno de los pasos de detail usar el campo con el mismo nombre en detail.
billing_account_id: en ocasiones es posible identificar al cliente en alguno de los pasos de detail usar el campo con el mismo nombre en detail. masiva_lg: si una llamada pasa por el módulo con nombre AVERIA_MASIVA se indicará con un 1 en este flag, de lo contrario llevará un 0. info_by_phone_lg: si una llamada pasa por el step de nombre CUSTOMERINFOBYPHONE.TX y su step_description_error es NULL, quiere decir que hemos podido identificar al cliente a través de su número de teléfono. En ese caso pondremos un 1 ente flag, de lo contrario llevará un 0. info_by_dni_lg: si una llamada pasa por el step de nombre CUSTOMERINFOBYDNI.TX y su step_description_error es NULL, quiere decir que hemos podido identificar al cliente a través de su número de DNI. En ese caso pondremos un 1 ente flag, de lo contrario llevará un 0. repeated_phone_24H: es un flag (0 o 1) que indica si ese mismo número ha realizado una llamada en las 24h anteriores. cause_recall_phone_24H: es un flag (0 o 1) que indica si ese mismo número ha realizado una llamada en las 24h posteriores.
Entregar el código SQL que generaría la tabla ivr_summary dentro del dataset keepcoding.
*/

--Creamos la tabla ivr_summary (tabla resumen)
CREATE OR REPLACE TABLE
  `keepcoding.ivr_summary` AS
WITH
-- empezamos recogiendo los campos importantes de una llamada como tal, que es su ID y el teléfono
  llamadas AS (
  SELECT
    i_c.ivr_id,
    i_c.phone_number
  FROM
    `keepcoding.ivr_calls` i_c),
  cliente AS(
-- continuamos recogiendo ahora los datos relevanters del cliente dentro de la llamada
-- también se controla si los identificadores del cliente o campos relevantes sean nulos o no      
  SELECT
    i_d.ivr_id,
    NULLIF (i_d.document_type,"NULL") AS document_type,
    NULLIF (i_d.document_identification,"NULL") AS document_identification,
    NULLIF (i_d.customer_phone,"NULL") AS customer_phone,
    NULLIF (i_d.billing_account_id,"NULL") AS billing_account_id
  FROM
    `keepcoding.ivr_detail` i_d
  GROUP BY
    i_d.ivr_id,
    i_d.document_type,
    i_d.document_identification,
    i_d.customer_phone,
    i_d.billing_account_id)

-- Una vez que hemos "filtrado" todos esos datos de una llamada los vamos a utilizar para recoger todos los datos de la llamada que solicita el ejercicio
SELECT
  i_d.ivr_id,
  i_d.phone_number,
  i_d.ivr_result,

-- en este caso se va a editar el valor que venga en un label por los valores que se quieren tener en la consulta
IF
  (LEFT(i_d.vdn_label, 4) = 'TECH', 'TECH',
  IF
    (i_d.vdn_label LIKE 'ABSORTION%', 'ABSORTION', 'RESTO')) AS vdn_aggregation,

-- Se continua cogiendo más valores de la llamada y del cliente
  i_d.start_date,
  i_d.end_date,
  i_d.total_duration,
  i_d.customer_segment,
  i_d.ivr_language,
  i_d.steps_module,
  i_d.module_aggregation,
  cl.document_type,
  cl.document_identification,
  cl.customer_phone,
  cl.billing_account_id,

-- a partir de estos campos, se va a recoger la cierta información de la llamada de los campos preparados anteriormente 'cliente' junto con los de detalles, motivos, para hacer un "check" por el motivo de esta misma
  CASE
    WHEN i_d.step_name LIKE '%AVERIA_MASIVA%' THEN 1
  ELSE
    0
  END AS masiva_lg,
  CASE
    WHEN i_d.step_name LIKE '%CUSTOMERINFOBYPHONE.TX%' AND i_d.step_description_error IS NULL THEN 1
  ELSE
    0
  END AS info_by_phone_lg,
  CASE
    WHEN i_d.step_name LIKE '%CUSTOMERINFOBYDNI.TX%' AND i_d.step_description_error IS NULL THEN 1
  ELSE
    0
  END AS info_by_dni_lg,

-- en este punto se va a controlar si se ha llamado en primera instancia, más veces en las últimas 24h, y /o posteriormente en las siguientes 24h
  CASE
    WHEN i_d.phone_number IN ( SELECT DISTINCT phone_number FROM `keepcoding.ivr_detail` WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), end_date, HOUR) <= 24 ) THEN 1
  ELSE
    0
  END AS repeated_phone_24H,
  CASE
    WHEN i_d.phone_number IN ( SELECT DISTINCT phone_number FROM `keepcoding.ivr_detail` WHERE TIMESTAMP_DIFF(end_date, CURRENT_TIMESTAMP(), HOUR) <= 24 ) THEN 1
  ELSE
    0
  END AS cause_recall_phone_24H

-- Finalmente hacemos JOIN de las tablas tanto creadas al vuelo como las existentes para que "macheen" todos los datos que se necesitan recoger
FROM
  cliente cl
LEFT JOIN
  `keepcoding.ivr_detail` i_d
ON
  cl.ivr_id = i_d.ivr_id
LEFT JOIN
  `keepcoding.ivr_steps` st
ON
  i_d.ivr_id = st.ivr_id
LEFT JOIN
  llamadas
ON
  i_d.ivr_id = llamadas.ivr_id

-- agrupamos por los campos que se han recogido
GROUP BY
  i_d.ivr_id,
  i_d.phone_number,
  i_d.ivr_result,
  vdn_aggregation,
  i_d.start_date,
  i_d.end_date,
  i_d.total_duration,
  i_d.customer_segment,
  i_d.ivr_language,
  i_d.steps_module,
  i_d.step_name,
  i_d.step_description_error,
  i_d.module_aggregation,
  cl.document_type,
  cl.document_identification,
  cl.customer_phone,
  cl.billing_account_id