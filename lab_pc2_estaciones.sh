#!/bin/bash
# =============================================================================
# SCRIPT UBUNTU/LINUX PARA ORDENADOR 2: ESTACIONES DE RIEGO (WS_01 y WS_02)
# =============================================================================
IP_CENTRAL=$1
if [ -z "$IP_CENTRAL" ]; then
    read -p "Introduce la IP de AWS o del Ordenador 1 (donde corre Central y Kafka): " IP_CENTRAL
fi
if [ -z "$IP_CENTRAL" ]; then
    IP_CENTRAL="localhost"
fi

CP=".:lib/*:kafka-clients.jar:slf4j-api.jar:slf4j-simple.jar"

echo "================================================================="
echo " Conectando WS_01 y WS_02 contra Central ($IP_CENTRAL:8080) y Kafka ($IP_CENTRAL:9092)"
echo "================================================================="

if command -v gnome-terminal &> /dev/null; then
    gnome-terminal --title="MONITOR WS_01" -- bash -c "java -cp '$CP' WM_WS_M 5001 $IP_CENTRAL 8080 WS_01; exec bash"
    sleep 1
    gnome-terminal --title="ENGINE WS_01" -- bash -c "java -cp '$CP' WM_WS_E $IP_CENTRAL 9092 localhost 5001 WS_01; exec bash"
    sleep 1
    gnome-terminal --title="MONITOR WS_02" -- bash -c "java -cp '$CP' WM_WS_M 5002 $IP_CENTRAL 8080 WS_02; exec bash"
    sleep 1
    gnome-terminal --title="ENGINE WS_02" -- bash -c "java -cp '$CP' WM_WS_E $IP_CENTRAL 9092 localhost 5002 WS_02; exec bash"
else
    echo "Ejecuta cada uno en una pestaña de terminal:"
    echo "  java -cp \"$CP\" WM_WS_M 5001 $IP_CENTRAL 8080 WS_01"
    echo "  java -cp \"$CP\" WM_WS_E $IP_CENTRAL 9092 localhost 5001 WS_01"
    echo "  java -cp \"$CP\" WM_WS_M 5002 $IP_CENTRAL 8080 WS_02"
    echo "  java -cp \"$CP\" WM_WS_E $IP_CENTRAL 9092 localhost 5002 WS_02"
fi
