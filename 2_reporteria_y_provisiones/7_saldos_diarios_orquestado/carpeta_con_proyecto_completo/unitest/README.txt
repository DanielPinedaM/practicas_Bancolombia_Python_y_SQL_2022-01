MINOS - THE PRODUCTION TESTER.

Descripción:

Minos es un proyecto que permite personalizar y calendarizar pruebas unitarias de calidad basandose en la librería pytest de python, 
las pruebas se ejecutarán con la periodicidad especificada y automáticamente se enviará un correo con los resultados a las personas interesadas.

Modo de uso:

1. En la carpeta "test" se guardan las distintas pruebas unitarias que se desean ejecutar en producción, estas pruebas se programan de acuerdo a la
librería unittest de python, mayor información en https://docs.python.org/3/library/unittest.html

2. En la carpeta "config" se modifica el archivo params.json, para indicar el nombre de la prueba, el responsable de la prueba  y los correos
de las personas interesadas en recibir los resultados.

3. Modificar el archivo scheduler.bat para indicar la periodicidad con la que desea que se ejecuten las pruebas.

4. Ejecutar el archivo scheduler.bat para que las pruebas queden calendarizadas en el programador de tareas del equipo.

Nota: 

Como este es un programa que se ejecuta automáticamente por el programador de tareas es importante que el equipo esté en funcionamiento 
a la hora en la que se programó la ejecución.