# Guía rápida: módulos e imports en Python

Esta guía sirve como recordatorio práctico para organizar e importar
módulos en proyectos Python, especialmente cuando el proyecto crece y
tiene paquetes hermanos como `etl`, `storage` y `config`.

## 1. Estructura de referencia

Supongamos este proyecto:

``` text
etl_v1/
├── etl/
│   ├── __init__.py
│   ├── main_v2.py
│   ├── extract_raw.py
│   └── load_bronze_v2.py
│
├── storage/
│   ├── __init__.py
│   └── blob_client_v2.py
│
├── config/
│   ├── __init__.py
│   └── settings.py
│
└── .venv/
```

La carpeta `etl_v1/` es la **raíz del proyecto**.

------------------------------------------------------------------------

## 2. Fórmula para recordar un import

La forma general es:

``` python
from paquete.modulo import elemento
```

Ejemplo:

``` python
from storage.blob_client_v2 import upload_file
```

Se interpreta así:

``` text
storage          -> paquete/carpeta
blob_client_v2   -> módulo/archivo blob_client_v2.py
upload_file      -> función, clase o variable importada
```

> Regla mental: **paquete → módulo → elemento**.

------------------------------------------------------------------------

## 3. Convertir una ruta en un import

Piensa en la ruta desde la raíz del proyecto y reemplaza `/` por `.`.

``` text
etl/extract_raw.py
        ↓
etl.extract_raw
```

Por tanto:

``` python
from etl.extract_raw import extract_table
```

Otro ejemplo:

``` text
storage/blob_client_v2.py
        ↓
storage.blob_client_v2
```

Por tanto:

``` python
from storage.blob_client_v2 import upload_file
```

Y:

``` text
config/settings.py
        ↓
config.settings
```

Por tanto:

``` python
from config.settings import DATABASE_URL
```

------------------------------------------------------------------------

## 4. Imports recomendados para el proyecto

En `etl/main_v2.py`:

``` python
from etl.extract_raw import extract_table
from etl.load_bronze_v2 import load_to_bronze
```

En `etl/load_bronze_v2.py`:

``` python
from storage.blob_client_v2 import upload_file
```

Si necesitamos configuración:

``` python
from config.settings import DATABASE_URL
```

La idea es utilizar **imports absolutos desde la raíz del proyecto**.

------------------------------------------------------------------------

## 5. Cómo ejecutar correctamente

Primero sitúate en la raíz:

``` bash
cd /ruta/al/proyecto/etl_v1
```

Luego ejecuta el módulo:

``` bash
python -m etl.main_v2
```

Con `-m`, Python espera un **nombre de módulo**, no una ruta de archivo.

Por eso:

``` bash
# ❌ Incorrecto
python -m etl/main_v2.py

# ❌ Incorrecto
python -m etl/main_v2

# ✅ Correcto
python -m etl.main_v2
```

Regla:

``` text
Ruta del archivo          Nombre del módulo

etl/main_v2.py     --->   etl.main_v2
storage/test.py    --->   storage.test
```

> Con `python -m`: **usa puntos y elimina `.py`**.

------------------------------------------------------------------------

## 6. ¿Por qué puede fallar `python etl/main_v2.py`?

Si ejecutas:

``` bash
python etl/main_v2.py
```

Python trata el directorio que contiene el script de una manera
diferente al resolver imports. Esto puede provocar errores al importar
paquetes hermanos:

``` text
ModuleNotFoundError: No module named 'storage'
```

En un proyecto organizado mediante paquetes, es preferible ejecutar
desde la raíz:

``` bash
python -m etl.main_v2
```

Así Python puede resolver correctamente:

``` text
etl_v1/
├── etl
├── storage
└── config
```

------------------------------------------------------------------------

## 7. Imports relativos: `.` y `..`

Python también permite imports relativos.

Un punto:

``` python
from .extract_raw import extract_table
```

significa:

``` text
. = paquete actual
```

Dos puntos:

``` python
from ..otro_paquete import algo
```

significan subir al paquete padre.

Sin embargo, los imports relativos dependen de que el código se ejecute
dentro del contexto correcto de un paquete. Por eso puede aparecer:

``` text
ImportError: attempted relative import with no known parent package
```

Para proyectos como este ETL, inicialmente es más sencillo mantener
imports absolutos:

``` python
from etl.extract_raw import extract_table
from storage.blob_client_v2 import upload_file
```

------------------------------------------------------------------------

## 8. Algo que nunca debemos hacer

Esto parece una ruta del sistema de archivos, pero **no es sintaxis
válida de importación Python**:

``` python
# ❌ Incorrecto
from ../storage.blob_client import upload_file
```

Python no interpreta un `import` como si fuera:

``` bash
cd ../
```

La sintaxis relativa válida utiliza puntos:

``` python
from ..storage.blob_client import upload_file
```

pero solo cuando la estructura de paquetes hace válido ese import
relativo.

------------------------------------------------------------------------

## 9. Evitar modificar `sys.path` manualmente

Puedes encontrar soluciones como:

``` python
import sys
sys.path.append("../")
```

o:

``` python
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))
```

Aunque pueden funcionar, conviene evitarlas para solucionar la
estructura normal de imports de una aplicación.

Es preferible:

1.  Organizar correctamente los paquetes.
2.  Utilizar imports absolutos.
3.  Ejecutar desde la raíz con `python -m`.

------------------------------------------------------------------------

## 10. Checklist cuando aparece `ModuleNotFoundError`

Cuando veas:

``` text
ModuleNotFoundError: No module named 'xxx'
```

comprueba:

1.  **¿Estoy situado en la raíz del proyecto?**

    ``` bash
    pwd
    ```

2.  **¿Estoy ejecutando como módulo?**

    ``` bash
    python -m etl.main_v2
    ```

3.  **¿El import empieza desde un paquete reconocible desde la raíz?**

    ``` python
    from storage.blob_client_v2 import upload_file
    ```

4.  **¿Mis paquetes tienen `__init__.py`?**

    ``` text
    etl/__init__.py
    storage/__init__.py
    config/__init__.py
    ```

    En Python moderno no siempre es obligatorio, pero mantenerlo
    explícito ayuda a dejar clara la intención de que esos directorios
    sean paquetes.

5.  **¿Estoy usando `/` donde debería usar `.`?**

    ``` text
    ❌ etl/main_v2
    ✅ etl.main_v2
    ```

6.  **¿Estoy incluyendo `.py` con `python -m`?**

    ``` text
    ❌ python -m etl.main_v2.py
    ✅ python -m etl.main_v2
    ```

------------------------------------------------------------------------

## 11. Chuleta de 20 segundos

``` text
ESTRUCTURA
etl_v1/
├── etl/
│   └── main_v2.py
├── storage/
│   └── blob_client_v2.py
└── config/
    └── settings.py


IMPORTS
from etl.extract_raw import extract_table
from storage.blob_client_v2 import upload_file
from config.settings import DATABASE_URL


EJECUCIÓN
cd etl_v1
python -m etl.main_v2


RECORDAR
ruta/carpeta/archivo.py
        ↓
ruta.carpeta.archivo


Con -m:
❌ python -m etl/main_v2.py
❌ python -m etl/main_v2
✅ python -m etl.main_v2
```

## Regla de oro

> **Ruta de carpetas → puntos. Archivo `.py` → módulo. Ejecutar desde la
> raíz → `python -m paquete.modulo`.**

Cuando el proyecto crezca, esta convención hará que los imports sean
mucho más predecibles y evitará muchos `ModuleNotFoundError`.
