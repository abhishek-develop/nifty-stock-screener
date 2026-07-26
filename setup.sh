#!/bin/bash
# Indian Stock VCP Scanner - Setup Script (Unix/Mac)
# Run: bash setup.sh

echo "🇮🇳 Indian Stock VCP Scanner - Setup"
echo "======================================"
echo ""

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 not found. Please install Python 3.8+"
    exit 1
fi

PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
echo "✓ Python version: $PYTHON_VERSION"

# Create virtual environment
echo ""
echo "📦 Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
echo ""
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Test import
echo ""
echo "🧪 Testing imports..."
python3 -c "import flask, yfinance, pandas, numpy, apscheduler; print('✓ All imports successful')"

echo ""
echo "✅ Setup complete!"
echo ""
echo "To start the scanner:"
echo "   source venv/bin/activate"
echo "   python vcp_scanner_web.py"
echo ""
echo "Then open: http://localhost:8000"
