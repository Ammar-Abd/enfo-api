#!/bin/bash
# This is a Bash script to run API commands

# Print a message to track when the script starts
echo "Starting API, Data Recovery and Chatbot  automation at $(date)"

# Start the Plumber API (R script) in the background
echo "Starting Plumber API at $(date)"
Rscript /srv/shiny-server/EnvizAPI/api.R &
# Store the process ID
PLUMBER_PID=$!

# Start the Python Data Recovery API in the background
echo "Starting Data Recovery API at $(date)"
python3 '/srv/shiny-server/Clients Usage Data/Data Recovery/pyServerA.py' &
# Store the process ID
DATA_RECOVERY_PID=$!

# Start the Python Chatbot API in the background
echo "Starting Chatbot API at $(date)"
python3 /srv/shiny-server/EnvizFleet/chatbot.py &
# Store the process ID
CHATBOT_PID=$!

# Wait briefly to ensure APIs start
sleep 2

# Keep the script running until all APIs finish
wait $PLUMBER_PID $DATA_RECOVERY_PID $CHATBOT_PID
