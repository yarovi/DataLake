# Diagnóstico técnico: táctica y algoritmo mental

## Idea central

No memorices soluciones aisladas. Aprende a **aislar la capa que
falla**.

## Caso Spark que usamos

El flujo era:

``` text
Spark → Ivy → hadoop-azure → Azurite → Bronze → Parquet
```

El error mostraba `FileNotFoundException` sobre
`/nonexistent/.ivy2.5.2/cache`. Eso indicaba que el fallo ocurría en Ivy
y su filesystem, antes de Azure, Azurite o Parquet.

La prueba mínima fue cambiar la caché de Ivy a una ubicación escribible
(`/tmp/ivy`). Una vez validada esa capa, Spark pudo resolver
`hadoop-azure` y sus dependencias transitivas.

## Táctica reutilizable

``` text
DESCUBRIR → AISLAR → PROBAR → VALIDAR → PERSISTIR → INTEGRAR
```

### Descubrir

Obtén información del runtime real: versiones, componentes instalados,
rutas y configuración. No confíes primero en un tutorial.

### Aislar

Busca `Exception`, `Caused by` o el primer mensaje concreto. Determina
qué capa lo produjo.

### Clasificar

-   `FileNotFoundException` → ruta, filesystem o permisos.
-   `ClassNotFoundException` / `NoClassDefFoundError` → dependencia o
    classpath.
-   `Connection refused` / timeout → servicio, red o puerto.
-   `UnknownHost` → DNS/hostname/red.
-   401/403 → autenticación/autorización.
-   Errores de schema/query → datos o transformación.

### Probar

Construye la prueba más pequeña posible. No pruebes seis componentes
nuevos simultáneamente.

### Cambiar una sola variable

Si sospechas de Ivy, cambia Ivy; no cambies además Hadoop, Azure, red y
código.

### Validar

Busca evidencia positiva, no solo ausencia del error: dependencia
encontrada, JAR cargado, aplicación iniciada, resultado esperado y
salida exitosa.

### Persistir

Primero valida una solución temporal. Después conviértela en
configuración reproducible (`compose.yml`, variables, imagen,
configuración, etc.).

## Algoritmo mental

``` text
ERROR
  ↓
¿Cuál es la causa concreta?
  ↓
¿Qué tipo de error es?
  ↓
¿Qué componente controla ese recurso?
  ↓
¿Cuál es la prueba mínima?
  ↓
Cambiar UNA variable
  ↓
Ejecutar
  ↓
¿La evidencia confirma la hipótesis?
  ├─ NO → siguiente hipótesis
  └─ SÍ → persistir e integrar
```

## Regla para recordar

> No persigas las 200 líneas del error. Encuentra la causa concreta,
> identifica la capa, reduce el problema y demuestra cada hipótesis con
> una prueba pequeña.

## Checklist

1.  ¿Cuál es la primera excepción útil?
2.  ¿Menciona archivo, clase, host, puerto, credencial o columna?
3.  ¿Qué componente es responsable?
4.  ¿Estoy diagnosticando la capa correcta?
5.  ¿Puedo reproducirlo con una prueba menor?
6.  ¿Estoy cambiando una sola variable?
7.  ¿Qué evidencia demostraría que acerté?
8.  ¿La solución temporal debe convertirse en configuración
    reproducible?

Este método es aplicable a Spark, Python, Java, Spring Boot,
Docker/Podman, Kubernetes, bases de datos y sistemas distribuidos.
