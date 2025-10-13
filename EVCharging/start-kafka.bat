@echo off
echo ========================================
echo    INICIANDO KAFKA + BD PARA EVCHARGING
echo ========================================
echo.

echo 1. Iniciando Zookeeper y Kafka...
docker-compose up -d

echo.
echo 2. Esperando 30 segundos para que Kafka este listo...
timeout 30

echo.
echo 3. Creando topics de Kafka...
docker exec kafka kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka kafka-topics.sh --create --topic cp-registration --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka kafka-topics.sh --create --topic charging-request --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka kafka-topics.sh --create --topic cp-health --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

echo.
echo 4. Mostrando topics creados:
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092

echo.
echo ========================================
echo     KAFKA LISTO EN localhost:9092
echo     BD SQLite se creará al ejecutar las apps
echo     Ahora ejecuta tus aplicaciones Java
echo ========================================
echo.
pause