#!/bin/bash
# Script to check if api.R is running on port 8000 and restart if not

# Set working directory
cd /srv/shiny-server/EnvizAPI

# Check if port 8000 is in use
if lsof -i :8000 > /dev/null; then
    echo "Port 8000 is in use, api.R is running"
else
    echo "Port 8000 is free, restarting api.R at $(date)"
    Rscript api.R &
    echo "api.R started with PID $!"
fi
