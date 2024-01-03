MINOS - THE PRODUCTION TESTER.

Descripci�n:

Minos es un proyecto que permite personalizar y calendarizar pruebas unitarias de calidad basandose en la librer�a pytest de python, 
las pruebas se ejecutar�n con la periodicidad especificada y autom�ticamente se enviar� un correo con los resultados a las personas interesadas.

Modo de uso:

1. En la carpeta "test" se guardan las distintas pruebas unitarias que se desean ejecutar en producci�n, estas pruebas se programan de acuerdo a la
librer�a unittest de python, mayor informaci�n en https://docs.python.org/3/library/unittest.html

2. En la carpeta "config" se modifica el archivo params.json, para indicar el nombre de la prueba, el responsable de la prueba  y los correos
de las personas interesadas en recibir los resultados.

3. Modificar el archivo scheduler.bat para indicar la periodicidad con la que desea que se ejecuten las pruebas.

4. Ejecutar el archivo scheduler.bat para que las pruebas queden calendarizadas en el programador de tareas del equipo.

Nota: 

Como este es un programa que se ejecuta autom�ticamente por el programador de tareas es importante que el equipo est� en funcionamiento 
a la hora en la que se program� la ejecuci�n.