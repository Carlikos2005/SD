@echo off
echo ========================================
echo    EVCHARGING - CON DOCKER PARA TODO
echo ========================================
echo.

echo 1. Navegando a la carpeta del script...
cd /d "%~dp0"

echo 2. Iniciando Kafka con Docker...
docker-compose up -d

echo Esperando 15 segundos para que Kafka este listo...
timeout 15

echo 3. Compilando y ejecutando con Docker...
echo.
echo EJECUTA EN TERMINALES SEPARADAS:
echo.
echo Terminal 1 - CENTRAL:
echo   docker run -it --rm -v "%cd%":/app -w /app maven:3.8.5-openjdk-11 mvn exec:java -Dexec.mainClass="EV_Central" -Dexec.args="8081 kafka:9092"
echo.
echo Terminal 2 - CP ENGINE:
echo   docker run -it --rm -v "%cd%":/app -w /app maven:3.8.5-openjdk-11 mvn exec:java -Dexec.mainClass="EV_CP_E" -Dexec.args="kafka:9092 CP-001 DRIVER-001"
echo.
echo Terminal 3 - CP MONITOR:
echo   docker run -it --rm -v "%cd%":/app -w /app maven:3.8.5-openjdk-11 mvn exec:java -Dexec.mainClass="EV_CP_M" -Dexec.args="ev_central 8081 kafka:9092 CP-001"
echo.
echo Terminal 4 - DRIVER:
echo   docker run -it --rm -v "%cd%":/app -w /app maven:3.8.5-openjdk-11 mvn exec:java -Dexec.mainClass="TestClient" -Dexec.args="kafka:9092 DRIVER-001"
echo.
pause