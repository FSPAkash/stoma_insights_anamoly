#!/usr/bin/env bash
set -euo pipefail

# Install Python dependencies
pip install -r backend/requirements.txt

# Build React frontend
cd frontend
npm install
REACT_APP_API_URL=/api npm run build
cd ..
