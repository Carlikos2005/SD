@echo off
echo ========================================
echo    CONSTRUYENDO Y EJECUTANDO EVCHARGING
echo ========================================
echo.

echo 1. Compilando proyecto...
mvn clean compile

if %ERRORLEVEL% NEQ 0 (
    echo Error compilando
    pause
    exit /b 1
)

echo.
echo 2. Compilación exitosa
echo 3. Ahora ejecuta en terminales separadas:
echo.
echo    Terminal 1: mvn exec:java -Dexec.mainClass="EV_Central" -Dexec.args="8081 localhost:9092"
echo    Terminal 2: mvn exec:java -Dexec.mainClass="EV_CP_E" -Dexec.args="localhost:9092 CP-001 DRIVER-001"  
echo    Terminal 3: mvn exec:java -Dexec.mainClass="EV_CP_M" -Dexec.args="localhost:9092 CP-001"
echo    Terminal 4: mvn exec:java -Dexec.mainClass="TestClient" -Dexec.args="localhost:9092 DRIVER-001"
echo.
pause