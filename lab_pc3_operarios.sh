#!/bin/bash
# =============================================================================
# SCRIPT UBUNTU/LINUX PARA ORDENADOR 3: OPERARIOS DE CAMPO (FO_01 y FO_02)
# =============================================================================
IP_KAFKA=$1
if [ -z "$IP_KAFKA" ]; then
    read -p "Introduce la IP de AWS o del Ordenador 1 (donde corre Kafka): " IP_KAFKA
fi
if [ -z "$IP_KAFKA" ]; then
    IP_KAFKA="localhost"
fi

CP=".:lib/*:kafka-clients.jar:slf4j-api.jar:slf4j-simple.jar"

echo "================================================================="
echo " Conectando FO_01 y FO_02 contra Kafka ($IP_KAFKA:9092)"
echo "================================================================="

if command -v gnome-terminal &> /dev/null; then
    gnome-terminal --title="OPERARIO FO_01" -- bash -c "java -cp '$CP' WM_FO $IP_KAFKA 9092 FO_01; exec bash"
    sleep 1
    gnome-terminal --title="OPERARIO FO_02" -- bash -c "java -cp '$CP' WM_FO $IP_KAFKA 9092 FO_02; exec bash"
else
    java -cp "$CP" WM_FO $IP_KAFKA 9092 FO_01
fi
