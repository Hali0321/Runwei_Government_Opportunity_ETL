#!/bin/bash

echo "🔧 SETTING UP COMPLETE AUTOMATION"
echo "================================="
echo "📅 Setup time: $(date)"

# Get current directory
CURRENT_DIR=$(pwd)
SCHEDULER_PATH="$CURRENT_DIR/production/scripts/daily_scheduler.sh"

echo "📍 Project location: $CURRENT_DIR"
echo "📍 Scheduler script: $SCHEDULER_PATH"

# Verify scheduler exists
if [ ! -f "$SCHEDULER_PATH" ]; then
    echo "❌ Error: Scheduler script not found"
    echo "Please ensure you're running from the project root directory"
    exit 1
fi

echo "✅ Scheduler script verified"

# Create cron job for daily execution at 6:00 AM
CRON_ENTRY="0 6 * * * cd $CURRENT_DIR && $SCHEDULER_PATH"

echo "🕕 Setting up daily cron job:"
echo "   Time: 6:00 AM daily"
echo "   Command: $CRON_ENTRY"

# Add to crontab (backing up existing crontab first)
echo ""
echo "📋 Current crontab entries:"
crontab -l 2>/dev/null || echo "No existing crontab"

echo ""
echo "🔄 Adding new cron job..."
(crontab -l 2>/dev/null; echo "$CRON_ENTRY") | crontab -

echo "✅ Cron job added successfully!"

echo ""
echo "📋 Updated crontab:"
crontab -l

echo ""
echo "🎊 AUTOMATION SETUP COMPLETE!"
echo "=============================="
echo "✅ Daily pipeline will run automatically at 6:00 AM"
echo "📊 Fresh grant data will be processed daily"
echo "🎯 Check logs/ directory for execution details"
echo "📧 Optionally configure email notifications in the scheduler"

echo ""
echo "🚀 TO TEST AUTOMATION NOW:"
echo "   ./production/scripts/daily_scheduler.sh"

echo ""
echo "⚙️ TO REMOVE AUTOMATION:"
echo "   crontab -e  # Remove the line with daily_scheduler.sh"

echo ""
echo "📁 IMPORTANT FILES:"
echo "   - Master controller: production/scripts/master_automation_controller.py"
echo "   - Daily scheduler: production/scripts/daily_scheduler.sh"
echo "   - Logs directory: logs/"
echo "   - Final data: FinalOpportunities table in SQL Database"

