@echo off
echo ========================================
echo    EJECUTANDO EVCHARGING - SIN MAVEN
echo ========================================
echo.

:: Navegar a la carpeta src/main/java
cd src\main\java

echo 1. Compilando proyectos...

:: Crear directorio para dependencias
if not exist lib mkdir lib

:: Descargar dependencias si no existen
echo Descargando dependencias...
if not exist "lib\kafka-clients-3.4.0.jar" (
  powershell -Command "Invoke-WebRequest -Uri 'https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar' -OutFile 'lib\kafka-clients-3.4.0.jar'"
)

if not exist "lib\jackson-databind-2.15.2.jar" (
  powershell -Command "Invoke-WebRequest -Uri 'https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.15.2/jackson-databind-2.15.2.jar' -OutFile 'lib\jackson-databind-2.15.2.jar'"
  powershell -Command "Invoke-WebRequest -Uri 'https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.15.2/jackson-core-2.15.2.jar' -OutFile 'lib\jackson-core-2.15.2.jar'"
  powershell -Command "Invoke-WebRequest -Uri 'https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.15.2/jackson-annotations-2.15.2.jar' -OutFile 'lib\jackson-annotations-2.15.2.jar'"
)

:: Compilar todos los .java
echo Compilando clases...
javac -cp ".;lib\*" *.java

if %ERRORLEVEL% NEQ 0 (
    echo Error en compilacion
    pause
    exit /b 1
)

:: Crear archivo requests.txt si no existe
if not exist requests.txt (
  echo CP-001 > requests.txt
  echo CP-002 >> requests.txt
)

echo.
echo ✅ COMPILACION EXITOSA!
echo.
echo 📝 EJECUTA EN TERMINALES SEPARADAS:
echo.
echo Terminal 1 - CENTRAL:
echo   java -cp ".;lib\*" EV_Central 8081 localhost:9092
echo.
echo Terminal 2 - CP ENGINE:
echo   java -cp ".;lib\*" EV_CP_E localhost:9092 CP-001 DRIVER-001
echo.
echo Terminal 3 - CP MONITOR:
echo   java -cp ".;lib\*" EV_CP_M localhost 8081 localhost:9092 CP-001
echo.
echo Terminal 4 - DRIVER:
echo   java -cp ".;lib\*" EV_Driver localhost 8081 DRIVER-001 requests.txt
echo.
echo 🎯 ORDEN RECOMENDADO: 1 → 3 → 2 → 4
echo.

pause