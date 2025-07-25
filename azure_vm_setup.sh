#!/bin/bash
# Azure VM Setup Script for Grants.gov ETL Pipeline
# Run this script on your Azure VM after connecting via SSH

set -e

echo "🚀 Starting Azure VM Setup for Grants.gov ETL Pipeline"
echo "======================================================="

# Update system
echo "📦 Updating system packages..."
sudo apt update && sudo apt upgrade -y

# Install Python 3.11 and dependencies
echo "🐍 Installing Python 3.11 and dependencies..."
sudo apt install -y python3.11 python3.11-venv python3-pip git curl nano htop

# Install SQL Server ODBC drivers
echo "🗄️  Installing SQL Server ODBC drivers..."
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt update
sudo ACCEPT_EULA=Y apt install -y msodbcsql18 unixodbc-dev

# Install Azure CLI (optional but recommended)
echo "☁️  Installing Azure CLI..."
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Clone repository
echo "📂 Cloning repository..."
cd ~
if [ -d "Runwei_Government_Opportunity_ETL" ]; then
    echo "Repository already exists, updating..."
    cd Runwei_Government_Opportunity_ETL
    git pull origin main
else
    git clone https://github.com/Hali0321/Runwei_Government_Opportunity_ETL.git
    cd Runwei_Government_Opportunity_ETL
fi

# Create Python virtual environment
echo "🔧 Setting up Python virtual environment..."
python3.11 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Create environment file template
echo "⚙️  Creating environment configuration..."
if [ ! -f .env ]; then
    cat > .env << 'EOF'
# Azure SQL Database Configuration
AZURE_SQL_SERVER=grants-gov-sql-server.database.windows.net
AZURE_SQL_DATABASE=GrantsGovDB
AZURE_SQL_USERNAME=grantsadmin
AZURE_SQL_PASSWORD=REPLACE_WITH_ACTUAL_PASSWORD

# Environment Settings
ENVIRONMENT=production
DEBUG=false
LOG_LEVEL=INFO

# Optional: Azure Storage (if used)
# AZURE_STORAGE_CONNECTION_STRING=REPLACE_WITH_CONNECTION_STRING
EOF
    echo "✅ Created .env file - PLEASE EDIT WITH ACTUAL CREDENTIALS!"
else
    echo "✅ .env file already exists"
fi

# Create production runner script
echo "🏭 Creating production ETL runner script..."
cat > ~/run_etl_production.sh << 'EOF'
#!/bin/bash
# Production ETL Pipeline Runner

set -e

# Configuration
ETL_DIR="/home/azureuser/Runwei_Government_Opportunity_ETL"
LOG_DIR="/home/azureuser/etl_logs"
VENV_PATH="$ETL_DIR/venv"
DATE=$(date '+%Y%m%d_%H%M%S')

# Create log directory
mkdir -p "$LOG_DIR"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_DIR/etl_production_$DATE.log"
}

log "🚀 Starting Grants.gov ETL Pipeline - Production"
log "📅 Execution Date: $(date)"
log "🖥️  Server: $(hostname)"

# Activate virtual environment
source "$VENV_PATH/bin/activate"

# Change to ETL directory
cd "$ETL_DIR/etl_pipeline"

# Run the complete ETL pipeline
log "🔄 Executing Bronze → Silver → Gold pipeline..."
python main.py 2>&1 | tee -a "$LOG_DIR/etl_production_$DATE.log"

# Check exit status
if [ $? -eq 0 ]; then
    log "✅ ETL Pipeline completed successfully!"
else
    log "❌ ETL Pipeline failed!"
    exit 1
fi

log "📊 Pipeline execution completed"
EOF

chmod +x ~/run_etl_production.sh

# Create monitoring script
echo "📊 Creating monitoring script..."
cat > ~/monitor_etl.sh << 'EOF'
#!/bin/bash
# ETL Pipeline Monitoring

LOG_DIR="/home/azureuser/etl_logs"
LATEST_LOG=$(ls -t "$LOG_DIR"/etl_production_*.log 2>/dev/null | head -1)

echo "🖥️  VM Status: grants-gov-etl-VM"
echo "📅 Current Time: $(date)"
echo ""

if [ -f "$LATEST_LOG" ]; then
    echo "📊 Latest ETL Execution:"
    echo "📁 Log: $(basename "$LATEST_LOG")"
    echo "📅 Last run: $(stat -c %y "$LATEST_LOG" | cut -d'.' -f1)"
    echo ""
    echo "🔍 Last 10 lines:"
    tail -10 "$LATEST_LOG"
    
    if grep -q "✅ ETL Pipeline completed successfully" "$LATEST_LOG"; then
        echo "✅ Status: SUCCESS"
    elif grep -q "❌ ETL Pipeline failed" "$LATEST_LOG"; then
        echo "❌ Status: FAILED"  
    else
        echo "⏳ Status: IN PROGRESS"
    fi
else
    echo "❓ No logs found - Pipeline will run at 8:00 AM EST daily"
fi

echo ""
echo "💾 Disk Usage: $(df -h /home/azureuser | tail -1)"
echo "🧠 Memory: $(free -h | grep '^Mem:' | awk '{print $3"/"$2}')"
EOF

chmod +x ~/monitor_etl.sh

# Set timezone to Eastern Time
echo "🕐 Setting timezone to America/New_York..."
sudo timedatectl set-timezone America/New_York

echo ""
echo "✅ Setup Complete!"
echo "=================="
echo ""
echo "Next steps:"
echo "1. Edit ~/.env with your actual database credentials"
echo "2. Test the database connection"
echo "3. Run a test ETL pipeline"
echo "4. Set up the daily cron job"
echo ""
echo "Commands to run next:"
echo "nano ~/Runwei_Government_Opportunity_ETL/.env"
echo "~/monitor_etl.sh"
echo "~/run_etl_production.sh"
