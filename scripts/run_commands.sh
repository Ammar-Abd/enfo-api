#!/bin/bash
# This is a Bash script to run Mosquitto commands on system startup

# Print a message to track when the script starts
echo "Starting Mosquitto automation at $(date)"

# Check Mosquitto status using sudo with password file
echo "Checking Mosquitto status at $(date)"
cat /home/eig-8/.sudo_password | sudo -S systemctl status mosquitto.service

# Stop Mosquitto service using sudo with password file
echo "Stopping Mosquitto service at $(date)"
cat /home/eig-8/.sudo_password | sudo -S systemctl stop mosquitto.service

# Start custom Mosquitto with full path
echo "Starting custom Mosquitto at $(date)"
/usr/sbin/mosquitto -c /etc/mosquitto/server.conf -v &
# Store the Mosquitto process ID
MOSQUITTO_PID=$!

# Keep the script running until Mosquitto finishes
wait $MOSQUITTO_PID
