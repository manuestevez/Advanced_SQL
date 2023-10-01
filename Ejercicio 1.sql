/*
Vamos a realizar el modelo de datos correspondiente a una IVR de atención al cliente.
Desde los ficheros ivr_calls, ivr_modules, e ivr_steps crear las tablas con los mismos nombres dentro del dataset keepcoding.
En ivr_calls encontramos los datos referentes a las llamadas.
En ivr_modules encontramos los datos correspondientes a los diferentes módulos por los que pasa la llamada. Se relaciona con la tabla de ivr_calls a través del campo ivr_id.
En ivr_steps encontramos los datos correspondientes a los pasos que da el usuario dentro de un módulo. Se relaciona con la tabla de módulos a través de los campos ivr_id y module_sequence.
Queremos tener los siguientes campos: calls_ivr_id calls_phone_number calls_ivr_result calls_vdn_label calls_start_date calls_start_date_id calls_end_date calls_end_date_id calls_total_duration calls_customer_segment calls_ivr_language calls_steps_module calls_module_aggregation module_sequece module_name module_duration module_result step_sequence step_name step_result step_description_error document_type document_identification
customer_phone billing_account_id
Los campos calls_start_date_id y calls_end_date_id son campos de fecha calculados, del tipo yyyymmdd. Por ejemplo, el 1 de enero de 2023 sería 20230101.
Entregar el código SQL que generaría la tabla ivr_detail dentro del dataset keepcoding.
*/

-- Creamos tabla dentro del dataset keepcoding
CREATE OR REPLACE TABLE
  `keepcoding.ivr_detail` AS
SELECT
  i_c.ivr_id,
  i_c.phone_number,
  i_c.ivr_result,
  i_c.vdn_label,
  i_c.start_date,
  FORMAT_DATE('%Y%m%d', i_c.start_date) AS start_date_id,
  i_c.end_date,
  FORMAT_DATE('%Y%m%d', i_c.end_date) AS end_date_id,
  i_c.total_duration,
  i_c.customer_segment,
  i_c.ivr_language,
  i_c.steps_module,
  i_c.module_aggregation,
  i_m.module_sequece,
  i_m.module_name,
  i_m.module_duration,
  i_m.module_result,
  i_s.step_sequence,
  i_s.step_name,
  i_s.step_result,
  i_s.step_description_error,
  i_s.document_type,
  i_s.document_identification,
  i_s.customer_phone,
  i_s.billing_account_id

--Desde la tabla de las llamadas
FROM
  `keepcoding.ivr_calls` i_c
  
--Relacionamos con ivr_modules
--En ivr_modules encontramos los datos correspondientes a los diferentes módulos por los que pasa la llamada. Se relaciona con la tabla de ivr_calls a través del campo ivr_id.
LEFT JOIN
  `keepcoding.ivr_modules` i_m
ON
  i_c.ivr_id = i_m.ivr_id 

--y relacionamos ivr_modules con ivr_steps
--En ivr_steps encontramos los datos correspondientes a los pasos que da el usuario dentro de un módulo. Se relaciona con la tabla de módulos a través de los campos ivr_id y module_sequence.
LEFT JOIN
  `keepcoding.ivr_steps` i_s
ON
  i_m.ivr_id = i_s.ivr_id
  AND i_m.module_sequece = i_s.module_sequece

--Finalmente agrupamos por lo que queremos sacar de resultado
GROUP BY
  i_c.ivr_id,
  i_c.phone_number,
  i_c.ivr_result,
  i_c.vdn_label,
  i_c.start_date,
  i_c.end_date,
  i_c.total_duration,
  i_c.customer_segment,
  i_c.ivr_language,
  i_c.steps_module,
  i_c.module_aggregation,
  i_m.module_sequece,
  i_m.module_name,
  i_m.module_duration,
  i_m.module_result,
  i_s.step_sequence,
  i_s.step_name,
  i_s.step_result,
  i_s.step_description_error,
  i_s.document_type,
  i_s.document_identification,
  i_s.customer_phone,
  i_s.billing_account_id
