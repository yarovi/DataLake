# Pruebas de integración --- Bronze → Staging → Apache Spark

## Objetivo

Documentar las pruebas realizadas para demostrar que los datos de
**Northwind** almacenados en la capa **Bronze de Azurite** pueden
descargarse a una zona de **staging** y ser procesados correctamente por
un cluster **Apache Spark Standalone**.

El objetivo de estas pruebas no es todavía transformar Bronze → Silver.
Primero se valida cada capa de infraestructura y movimiento de datos de
forma aislada.

## Flujo validado

``` text
PostgreSQL / Northwind
        |
        | Extract + Load (Python)
        v
Azurite / container bronze
        |
        | Azure Blob SDK - download
        v
Host macOS: ./staging/orders/orders.parquet
        |
        | Podman bind mount
        v
/opt/spark/staging/orders/orders.parquet
        |
        | spark.read.parquet(...)
        v
Spark DataFrame
        |
        +--> printSchema()
        +--> show(5)
        +--> count()
```

## 1. Validación Spark → Azurite

DNS interno:

``` bash
podman exec -it northwind-spark-master getent hosts azurite
```

Resultado: `azurite` fue resuelto mediante el DNS interno de Podman.

La prueba HTTP contra `http://azurite:10000` devolvió una respuesta de
`Azurite-Blob`. Un HTTP 400 era aceptable en esta prueba: demuestra que
DNS, red y puerto funcionan, aunque la petición no fuera una operación
Blob válida.

**Resultado:** DNS OK, puerto 10000 OK, Azurite accesible OK.

## 2. Diagnóstico del Spark Master

Durante las pruebas el contenedor terminó con `Exit (1)`. Los logs
mostraron:

``` text
Could not find or load main class org.apache.spark.deploy.master.Masters
ClassNotFoundException: org.apache.spark.deploy.master.Masters
```

Se inspeccionó el comando real del contenedor y se encontró
`org.apache.spark.deploy.master.Masters`. La clase correcta es
`org.apache.spark.deploy.master.Master`.

Corrección:

``` diff
- org.apache.spark.deploy.master.Masters
+ org.apache.spark.deploy.master.Master
```

### Algoritmo mental aplicado

``` text
síntoma
  ↓
container state improper
  ↓
podman ps / inspect
  ↓
Exit 1
  ↓
podman logs
  ↓
ClassNotFoundException
  ↓
identificar clase
  ↓
inspeccionar comando real
  ↓
comparar con Compose
  ↓
corregir solamente la causa
```

No fue necesario modificar Hadoop, Azurite, Python ni descargar JAR
adicionales.

## 3. Validación del cluster Spark

Después de la corrección, Spark Master inició en
`spark://spark-master:7077`, quedó `ALIVE` y registró el Worker.

Se validó:

``` text
Spark Master             OK
Spark Standalone :7077   OK
Master UI :8080          OK
Spark Worker             OK
Worker → Master          OK
```

## 4. Staging compartido

Se configuró un bind mount:

``` yaml
volumes:
  - spark_data:/opt/spark/work-dir
  - ./staging:/opt/spark/staging
```

Un **bind mount no copia** el archivo al contenedor. Expone el
directorio del host dentro del contenedor.

Prueba:

``` bash
echo "spark-staging-ok" > staging/test.txt

podman exec -it northwind-spark-master   cat /opt/spark/staging/test.txt
```

Resultado:

``` text
spark-staging-ok
```

## 5. Descarga Bronze → Staging

El Blob `bronze/orders/orders.parquet` se descargó mediante Azure Blob
SDK hacia `staging/orders/orders.parquet`.

``` text
Azurite
   |
   | DOWNLOAD
   v
./staging/orders/orders.parquet
   |
   | bind mount (sin nueva copia)
   v
/opt/spark/staging/orders/orders.parquet
```

El archivo local resultó de aproximadamente **36 KB**.

### Download vs copy vs bind mount

-   **Download:** transferencia desde un servicio de almacenamiento
    hacia el filesystem local.
-   **Copy:** duplicación de datos entre ubicaciones.
-   **Bind mount:** no duplica; expone un directorio del host dentro del
    contenedor.

## 6. Primera lectura real con PySpark

Archivo utilizado:

``` text
spark/read_bronze.py
```

Ejecución:

``` bash
podman exec -it northwind-spark-master   /opt/spark/bin/spark-submit   --master spark://spark-master:7077   /opt/spark/work-dir/read_bronze.py
```

La aplicación `northwind-read-bronze` se conectó al cluster Spark 4.2.0.
El Master asignó un executor en el Worker y éste quedó en estado
`RUNNING`.

## 7. Evidencia de ejecución distribuida

``` text
spark-submit
     ↓
Driver
     ↓
Spark Master
     ↓
Spark Worker
     ↓
Executor
     ↓
Tasks
```

Los logs muestran tareas ejecutándose en el executor del Worker. Esto
demuestra que no se trató simplemente de ejecutar un script Python
aislado.

## 8. Schema detectado desde Parquet

Spark recuperó correctamente:

``` text
root
 |-- order_id: long (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- employee_id: long (nullable = true)
 |-- order_date: date (nullable = true)
 |-- required_date: date (nullable = true)
 |-- shipped_date: date (nullable = true)
 |-- ship_via: long (nullable = true)
 |-- freight: double (nullable = true)
 |-- ship_name: string (nullable = true)
 |-- ship_address: string (nullable = true)
 |-- ship_city: string (nullable = true)
 |-- ship_region: string (nullable = true)
 |-- ship_postal_code: string (nullable = true)
 |-- ship_country: string (nullable = true)
```

Esto muestra una ventaja importante de **Parquet**: conserva información
de schema junto con los datos.

## 9. Validación de datos

`show(5)` devolvió órdenes reales de Northwind, incluyendo los IDs
10248, 10249, 10250, 10251 y 10252.

Por tanto:

``` text
Parquet accesible      OK
Schema interpretable   OK
Datos legibles         OK
Executor funcional     OK
```

## 10. Reconciliación

La acción:

``` python
orders_df.count()
```

devolvió:

``` text
Orders: 830
```

La reconciliación queda:

``` text
Origen PostgreSQL       830
       ↓
Bronze                   830
       ↓
Staging                  830
       ↓
Spark DataFrame          830
```

La **reconciliación de conteos** es una primera comprobación de
integridad: no hay evidencia de pérdida o multiplicación de filas
durante este recorrido.

## 11. Lazy Evaluation y ejecución

Los logs permiten observar cómo acciones como `show()` y `count()`
generan trabajo en Spark:

``` text
show()
  ↓
Job
  ↓
Stage
  ↓
Task
  ↓
Executor

count()
  ↓
Job / Stages
  ↓
Tasks
  ↓
830
```

Este concepto será fundamental al trabajar posteriormente con `select`,
`filter`, `withColumn`, `join` y `groupBy`.

## Estado actual del MVP 5

``` text
PostgreSQL → Bronze                       OK
Azurite Blob Storage                      OK
Bronze → staging                          OK
Bind mount host → Spark                   OK
Spark Master                              OK
Spark Worker                              OK
Driver → Master                           OK
Master → Executor                         OK
Lectura Parquet                           OK
Schema                                    OK
show(5)                                   OK
count() = 830                             OK
```

## Siguiente objetivo

La siguiente etapa será **Bronze → Silver**:

``` text
orders
customers
products
order_details
      ↓
Spark DataFrames
      ↓
validación y limpieza
      ↓
joins
      ↓
reglas de negocio
      ↓
Silver
```

Regla del laboratorio:

> Validar una capa y una hipótesis a la vez antes de agregar más
> complejidad.
