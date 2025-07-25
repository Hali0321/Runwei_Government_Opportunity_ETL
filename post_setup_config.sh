#!/bin/bash
# Post-Setup Configuration for Grants.gov ETL Pipeline
# Run this script AFTER editing the .env file with actual credentials

set -e

echo "🔧 Post-Setup Configuration for Grants.gov ETL Pipeline"
echo "======================================================"

# Navigate to project directory
cd ~/Runwei_Government_Opportunity_ETL

# Activate virtual environment
source venv/bin/activate

# Test database connection
echo "🗄️  Testing database connection..."
cd etl_pipeline
python -c "
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

try:
    from layers.bronze.scripts.run_layer1 import Layer1Processor
    processor = Layer1Processor()
    print('✅ Database connection successful!')
except Exception as e:
    print(f'❌ Database connection failed: {e}')
    print('Please check your .env file credentials')
    exit(1)
"

if [ $? -ne 0 ]; then
    echo "❌ Database connection test failed. Please check your .env file."
    exit 1
fi

# Test ETL pipeline components
echo "🧪 Testing ETL pipeline components..."
python -c "
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

try:
    # Test imports
    from layers.bronze.scripts.run_layer1 import Layer1Processor
    from layers.silver.scripts.run_layer2 import Layer2Processor
    from layers.gold.scripts.run_layer3 import Layer3Processor
    print('✅ All ETL components imported successfully!')
except Exception as e:
    print(f'❌ ETL component test failed: {e}')
    exit(1)
"

if [ $? -ne 0 ]; then
    echo "❌ ETL component test failed."
    exit 1
fi

# Create log directory
mkdir -p ~/etl_logs

# Set up daily cron job
echo "⏰ Setting up daily cron job (8:00 AM EST)..."
(crontab -l 2>/dev/null; echo "0 8 * * * /home/azureuser/run_etl_production.sh") | crontab -

echo "✅ Cron job scheduled successfully!"

# Display current cron jobs
echo "📅 Current scheduled jobs:"
crontab -l

echo ""
echo "🎉 Configuration Complete!"
echo "========================="
echo ""
echo "Your Grants.gov ETL Pipeline is now ready for production!"
echo ""
echo "Key Information:"
echo "• Daily execution: 8:00 AM EST (America/New_York timezone)"
echo "• Manual execution: ~/run_etl_production.sh"
echo "• Monitor status: ~/monitor_etl.sh"
echo "• Logs location: ~/etl_logs/"
echo ""
echo "Next steps:"
echo "1. Run a manual test: ~/run_etl_production.sh"
echo "2. Monitor the first run: ~/monitor_etl.sh"
echo "3. Check logs: ls -la ~/etl_logs/"
echo ""
echo "🚀 Your pipeline will automatically run daily at 8:00 AM EST!"
