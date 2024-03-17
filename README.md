# MP_Data_Eng
Test Mercado Pago


Original file is located at
    https://colab.research.google.com/drive/1V4JQ-QQalfu3M6-e9TLjFYpNEcG9mU3w

# ***MercadoPago***

**contexto**

En el contexto de MercadoPago, se quiere desarrollar un modelo de Machine Learning para
predecir el orden de un conjunto de Propuestas de Valor (aka, Value Props) en el carrusel
de la app llamado “Descubrí Más”.


***Tarea***

La tarea consiste en construir utilizando Python y sus librerías un pipeline que tenga como
input 3 fuentes de datos diferentes y genere como resultado un dataset listo para ser
ingerido por el modelo.

# ***Expected Result***

El dataset a construir deberá contar con la siguiente información:

● prints de la última semana

● por cada print:

     ○ un campo que indique si se hizo click o no

     ○ cantidad de veces que el usuario vio cada value prop en las 3 semanas previas a ese print.

     ○ cantidad de veces que el usuario clickeo cada value prop en las 3 semanas previas a ese print.

     ○ cantidad de pagos que el usuario realizó para cada value prop en las 3 semanas previas a ese print.

     ○ importes acumulados que el usuario gasto para cada value prop en las 3 semanas previas a ese print.

***Deliverables***

    ● Código Python
    ● Un Doc con una breve descripción de las decisiones tomadas.

***Mercado Pago - Data Eng***

    ● Este proyecto se desarrollo en diferentes procesos:

     Este proyecto se conformo de varias partes con la intencion de mostrar la experiencia en las teconologias con las que he trabajado y al mismo tiempo realizar la finalidad del proyecto.
    
     - Colab : Se desarrollo haciendo un pequeno ETL desde colab, se hizo usando spark para procesar la informacion. Los archivos para transformar estan guardados en un drive.
     - Jupyer Notebook : Este jupyter-Notebook esta dentro de un contenedor el cual esta conectado a una base de datos para ingresar la data y asemejarlo lo mas posible a realizar
                         un ETL. Este docker-compose crea a su vez una base de datos en postgres en la cual se ingresaran los datos manualmente.
     - FastAPI : Se crearon 6 GET's End-Points el cual mostrara en formato json la informacion guardada en la base de datos de postgres, esta informacion es traida mediante raw queries
                 dentro de fastapi.
               

***Como usar***

     ○ Descargar Repositorio

     ○ Descargar docker en caso de no tenerlo.

     ○ ajustar el archivo .env, seguir como ejemplo el env_template, introducir el PATH de donde se encuentra el archivo.

     ○ Si no tienes 'Make', instalarlo.

     ○ Ejecutar 'make up' esto creara los contenedores.

     ○ ingresar al localhost:8888 para el notebook de jupyter.

     ○ ingresar al localhost:8002 para el notebook de Swagger.

***Tree***

```
.
├── Makefile
├── README.md
├── __init__.py
├── docker-compose.yaml
├── env_template
├── fast_api_env
│   ├── Dockerfile
│   ├── __init__.py
│   ├── connection
│   │   ├── __init__.py
│   │   └── mysql_connection.py
│   ├── controllers
│   │   ├── __init__.py
│   │   └── controllers.py
│   ├── main.py
│   ├── poetry.lock
│   ├── pyproject.toml
│   ├── requirements.txt
│   └── services
│       ├── __init__.py
│       ├── db_service.py
│       └── queries.py
└── spark_env
    ├── Dockerfile
    ├── __init__.py
    ├── mercadoPago.ipynb
    ├── mercadolibre_sergioQuiroga.py
    ├── pays.csv
    ├── prints.json
    └── taps.json


```


***Transformación (Transform):***


* Limpieza de datos: eliminar datos duplicados, valores nulos inconsistencias, etc.
* Estandarización: convertir los datos en un formato uniforme y coherente.
* Normalización: estructurar los datos de manera que sigan un modelo predefinido.
* Enriquecimiento: añadir datos adicionales provenientes de otras fuentes para mejorar su calidad o contexto.
* Agregación: combinar y resumir datos para generar información más útil.
* Derivación: crear nuevos atributos o calcular métricas a partir de los datos existentes.
* Filtrado: eliminar datos no deseados o irrelevantes.



***Carga (Load):***

* En esta etapa, los datos transformados se cargan en el sistema de destino, que puede ser un almacén de datos, un data lake, una base de datos relacional, un sistema de almacenamiento en la nube, entre otros.


