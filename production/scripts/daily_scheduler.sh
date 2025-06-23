#!/bin/bash

echo "⏰ DAILY GRANTS PIPELINE SCHEDULER"
echo "=================================="
echo "📅 Execution time: $(date)"
echo "🎯 Running complete automated pipeline..."

# Set up environment
cd "$(dirname "$0")/.."
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Create daily log
LOG_DIR="logs"
mkdir -p "$LOG_DIR"
DAILY_LOG="$LOG_DIR/daily_execution_$(date +%Y%m%d).log"

# Execute master automation with comprehensive logging
{
    echo "🚀 Starting daily grants pipeline at $(date)"
    echo "============================================="
    
    python3 production/scripts/master_automation_controller.py
    
    PIPELINE_EXIT_CODE=$?
    
    echo ""
    echo "============================================="
    echo "🏁 Pipeline completed at $(date) with exit code: $PIPELINE_EXIT_CODE"
    
    if [ $PIPELINE_EXIT_CODE -eq 0 ]; then
        echo "✅ SUCCESS: Daily pipeline completed successfully"
        echo "📊 Fresh grant data is now available for your company"
        
        # Optional: Send success notification
        # echo "Daily grants pipeline completed successfully" | mail -s "Pipeline Success" your-email@domain.com
        
    else
        echo "❌ FAILURE: Daily pipeline failed"
        echo "📋 Check logs for troubleshooting information"
        
        # Optional: Send failure notification
        # echo "Daily grants pipeline failed - check logs" | mail -s "Pipeline Failed" your-email@domain.com
        
    fi
    
} 2>&1 | tee "$DAILY_LOG"

# Clean up old logs (keep last 30 days)
find "$LOG_DIR" -name "daily_execution_*.log" -mtime +30 -delete 2>/dev/null || true

exit $PIPELINE_EXIT_CODE
