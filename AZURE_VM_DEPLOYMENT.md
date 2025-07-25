# 🚀 AZURE VM DEPLOYMENT GUIDE
**Complete Production Setup for Grants.gov ETL Pipeline**

## 📋 Pre-Deployment Checklist

### ✅ Local Development Environment
- [x] **Repository ready** - All code committed and tested
- [x] **Dependencies updated** - requirements.txt includes all packages  
- [x] **Environment template** - .env.template configured
- [x] **Documentation complete** - SETUP.md and README.md updated
- [x] **Scheduler working** - Local testing successful
- [x] **Database cleanup** - Only 3 core tables remain
- [x] **GitHub ready** - All changes ready to push

### 🎯 Production Features Verified
- [x] **3-Layer ETL Pipeline** - Bronze → Silver → Gold architecture
- [x] **Daily Automation** - 8:00 AM EST scheduler configured
- [x] **Azure Integration** - SQL Database + Table Storage
- [x] **Error Handling** - Comprehensive logging and recovery
- [x] **Data Validation** - Quality checks at each layer
- [x] **Production Logging** - Structured logs with rotation

## 🖥️ Azure VM Setup Commands

### Step 1: VM Creation (Via Azure Portal)
```bash
# VM Configuration:
Resource Group: GrantsGov
VM Name: grants-gov-etl-VM  
Region: East US (same as SQL Database)
Image: Ubuntu 22.04 LTS
Size: Standard_D4s_v3 (4 vCPUs, 16GB RAM)
Authentication: SSH public key
Networking: Allow SSH (22), HTTP (80), HTTPS (443)
```

### Step 2: Initial VM Setup
```bash
# Connect to VM
ssh azureuser@YOUR_VM_IP

# Update system
sudo apt update && sudo apt upgrade -y

# Install Python 3.11 and dependencies
sudo apt install -y python3.11 python3.11-venv python3-pip git curl nano

# Install SQL Server ODBC drivers
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt update
sudo ACCEPT_EULA=Y apt install -y msodbcsql18 unixodbc-dev

# Install Azure CLI (optional)
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
```

### Step 3: Deploy Code
```bash
# Clone repository
git clone https://github.com/Hali0321/Runwei_Government_Opportunity_ETL.git
cd Runwei_Government_Opportunity_ETL

# Create Python environment
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure environment
cp .env.template .env
nano .env
```

### Step 4: Environment Configuration
```bash
# Edit .env with production values:
AZURE_SQL_SERVER=grants-gov-sql-server.database.windows.net
AZURE_SQL_DATABASE=GrantsGovDB
AZURE_SQL_USERNAME=grantsadmin
AZURE_SQL_PASSWORD=Grant$Admin2024!
ENVIRONMENT=production
DEBUG=false
LOG_LEVEL=INFO
```

### Step 5: Test Installation
```bash
# Test database connection
cd etl_pipeline
python -c "
import sys
sys.path.append('.')
from layers.bronze.scripts.run_layer1 import Layer1Processor
processor = Layer1Processor()
print('✅ Database connection successful!')
"

# Test scheduler
python scheduler.py --status
```

### Step 6: Production Automation Setup
```bash
# Create production runner script
nano ~/run_etl_production.sh
```

**Production Script Content:**
```bash
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
```

```bash
# Make executable
chmod +x ~/run_etl_production.sh
```

### Step 7: Daily Cron Job Setup
```bash
# Set timezone to EST
sudo timedatectl set-timezone America/New_York

# Open crontab
crontab -e

# Add daily 8:00 AM EST job
0 8 * * * /home/azureuser/run_etl_production.sh

# Optional: Run on startup  
@reboot sleep 120 && /home/azureuser/run_etl_production.sh
```

### Step 8: Monitoring Setup
```bash
# Create monitoring script
nano ~/monitor_etl.sh
```

**Monitoring Script:**
```bash
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
```

```bash
chmod +x ~/monitor_etl.sh
```

## 🔧 Management Commands

### Daily Operations
```bash
# Check pipeline status
~/monitor_etl.sh

# View real-time execution
tail -f ~/etl_logs/etl_production_*.log

# Manual pipeline run
~/run_etl_production.sh

# Check scheduled jobs
crontab -l

# System resource monitoring
htop
df -h
```

### Maintenance
```bash
# Update code from GitHub
cd ~/Runwei_Government_Opportunity_ETL
git pull origin main
source venv/bin/activate
pip install -r requirements.txt --upgrade

# Clean old logs (keep 30 days)
find ~/etl_logs -name "*.log" -mtime +30 -delete

# System updates (monthly)
sudo apt update && sudo apt upgrade -y
```

## 🎯 Expected Daily Operation

```
🕗 8:00 AM EST Daily Flow:
├── Cron triggers ~/run_etl_production.sh
├── Script activates Python virtual environment  
├── Executes Bronze Layer (5-6 minutes)
├── Executes Silver Layer (6-7 minutes)
├── Executes Gold Layer (<1 minute)
├── Updates database with 1,670+ fresh grants
├── Logs results to ~/etl_logs/
└── ✅ Completes by ~8:15 AM EST
```

## 🎊 Production Ready!

Your grants.gov ETL pipeline will be:
- ✅ **Fully automated** - Runs daily at 8:00 AM EST
- ✅ **Production scaled** - Handles 2,000+ grants efficiently  
- ✅ **Enterprise ready** - Comprehensive logging and monitoring
- ✅ **Maintenance friendly** - Easy updates and troubleshooting
- ✅ **Cloud optimized** - Azure SQL + VM integration

**Your database will be automatically updated every morning with the latest government grant opportunities!** 🚀
