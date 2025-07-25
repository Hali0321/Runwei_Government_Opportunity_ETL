# 🚀 Azure VM Management Guide
**Quick Reference for Your Grants.gov ETL Pipeline**

## 📋 Daily Operations

### Check Pipeline Status
```bash
~/monitor_etl.sh
```

### View Live Execution
```bash
# View real-time logs during execution
tail -f ~/etl_logs/etl_production_*.log

# View latest log file
ls -t ~/etl_logs/etl_production_*.log | head -1 | xargs cat
```

### Manual Pipeline Execution
```bash
# Run the full ETL pipeline manually
~/run_etl_production.sh

# Run individual layers for testing
cd ~/Runwei_Government_Opportunity_ETL/etl_pipeline
source ../venv/bin/activate

# Bronze layer only
python layers/bronze/scripts/run_layer1.py

# Silver layer only  
python layers/silver/scripts/run_layer2.py

# Gold layer only
python layers/gold/scripts/run_layer3.py
```

## 🔧 Maintenance Commands

### System Health Check
```bash
# Check disk space
df -h

# Check memory usage
free -h

# Check CPU usage
top

# Check system processes
htop

# Check scheduled jobs
crontab -l
```

### Update Application
```bash
# Update code from GitHub
cd ~/Runwei_Government_Opportunity_ETL
git pull origin main

# Update Python packages
source venv/bin/activate
pip install -r requirements.txt --upgrade
```

### Log Management
```bash
# View all log files
ls -la ~/etl_logs/

# View latest log
ls -t ~/etl_logs/etl_production_*.log | head -1 | xargs tail -50

# Clean old logs (keep 30 days)
find ~/etl_logs -name "*.log" -mtime +30 -delete

# Check log sizes
du -sh ~/etl_logs/*
```

### Environment Management
```bash
# Edit environment variables
nano ~/Runwei_Government_Opportunity_ETL/.env

# Test database connection
cd ~/Runwei_Government_Opportunity_ETL/etl_pipeline
source ../venv/bin/activate
python -c "
from layers.bronze.scripts.run_layer1 import Layer1Processor
processor = Layer1Processor()
print('✅ Database connection successful!')
"
```

## 🚨 Troubleshooting

### Common Issues

#### Pipeline Fails to Start
```bash
# Check if virtual environment is working
source ~/Runwei_Government_Opportunity_ETL/venv/bin/activate
python --version

# Check environment variables
cat ~/Runwei_Government_Opportunity_ETL/.env

# Test database connection
cd ~/Runwei_Government_Opportunity_ETL/etl_pipeline
python -c "import pyodbc; print('ODBC drivers:', [x for x in pyodbc.drivers()])"
```

#### Database Connection Issues
```bash
# Verify SQL Server is accessible
telnet grants-gov-sql-server.database.windows.net 1433

# Check firewall settings in Azure Portal
# Ensure VM's IP is in SQL Server firewall rules
```

#### Memory Issues
```bash
# Check available memory
free -h

# If low memory, consider upgrading VM size or adding swap
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

#### Cron Job Not Running
```bash
# Check if cron service is running
sudo systemctl status cron

# View cron logs
sudo grep CRON /var/log/syslog

# Test cron job manually
/home/azureuser/run_etl_production.sh
```

## 📊 Expected Performance

### Normal Execution Times
- **Bronze Layer**: 5-6 minutes (data collection)
- **Silver Layer**: 6-7 minutes (data processing)  
- **Gold Layer**: <1 minute (final aggregation)
- **Total Runtime**: ~12-15 minutes

### Resource Usage
- **Memory**: 2-4 GB during execution
- **CPU**: 50-80% during processing
- **Disk**: ~500MB per day for logs
- **Network**: ~100MB download during collection

## 🔔 Monitoring Alerts

### Success Indicators
- Log files contain "✅ ETL Pipeline completed successfully!"
- Database tables updated with fresh data
- No error messages in logs
- Execution completes within 15 minutes

### Failure Indicators  
- Log files contain "❌ ETL Pipeline failed!"
- Python traceback errors in logs
- Execution time > 30 minutes
- Missing log files for scheduled runs

## 📞 Emergency Contacts

### If Pipeline Fails
1. Check `~/monitor_etl.sh` for immediate status
2. Review latest log file for errors
3. Test database connection
4. Run manual execution to debug
5. Contact system administrator if persistent issues

### Azure Resources
- **SQL Server**: grants-gov-sql-server.database.windows.net
- **Database**: GrantsGovDB  
- **VM Resource Group**: GrantsGov
- **Region**: East US

---

**🎯 Remember**: The pipeline runs automatically every day at 8:00 AM EST. Manual intervention should only be needed for maintenance or troubleshooting.
