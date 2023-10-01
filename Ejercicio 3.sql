  /* 
  Crear una función de limpieza de enteros 
  por la que si entra un NULL la función devuelva el valor -999999.
  Entregar el código SQL que generaría la función clean_integer
  dentro del dataset keepcoding. 
  */

--dentro del dataset keepcoding, con el nombre function_clean_integer, al que se le pasará un valor
CREATE OR REPLACE FUNCTION `keepcoding.function_clean_integer` (valor_integer INT64)
  RETURNS INT64 AS ((
    SELECT
    --Dentro del SELECT utilizaremos la funcion IFNULL, para detectar si entra un null devolver -999999, sino el valor integer introducido
      IFNULL(valor_integer, -999999)));