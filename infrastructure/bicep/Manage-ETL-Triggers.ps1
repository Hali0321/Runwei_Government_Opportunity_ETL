# Azure Data Factory ETL Trigger Management Script
# This script helps manage the ETL triggers for testing and production

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("start-test", "stop-test", "start-daily", "stop-daily", "switch-to-daily", "status")]
    [string]$Action,
    
    [Parameter(Mandatory=$false)]
    [string]$ResourceGroupName = "grants-gov-rg",
    
    [Parameter(Mandatory=$false)]
    [string]$DataFactoryName = "df-grants-gov-etl",
    
    [Parameter(Mandatory=$false)]
    [string]$SubscriptionId
)

# Set subscription if provided
if ($SubscriptionId) {
    Write-Host "🔄 Setting subscription to: $SubscriptionId" -ForegroundColor Cyan
    az account set --subscription $SubscriptionId
}

$TestTriggerName = "ETL_Test_10Min_Trigger"
$DailyTriggerName = "ETL_Daily_8AM_Trigger"

function Write-StatusMessage {
    param([string]$Message, [string]$Color = "Green")
    Write-Host "📊 $Message" -ForegroundColor $Color
}

function Write-ErrorMessage {
    param([string]$Message)
    Write-Host "❌ $Message" -ForegroundColor Red
}

function Write-SuccessMessage {
    param([string]$Message)
    Write-Host "✅ $Message" -ForegroundColor Green
}

function Get-TriggerStatus {
    param([string]$TriggerName)
    
    try {
        $status = az datafactory trigger show `
            --resource-group $ResourceGroupName `
            --factory-name $DataFactoryName `
            --name $TriggerName `
            --query "properties.runtimeState" `
            --output tsv 2>$null
        
        if ($LASTEXITCODE -eq 0) {
            return $status
        } else {
            return "Not Found"
        }
    }
    catch {
        return "Error"
    }
}

function Start-Trigger {
    param([string]$TriggerName, [string]$Description)
    
    Write-StatusMessage "Starting $Description ($TriggerName)..."
    
    $result = az datafactory trigger start `
        --resource-group $ResourceGroupName `
        --factory-name $DataFactoryName `
        --name $TriggerName `
        --output none
    
    if ($LASTEXITCODE -eq 0) {
        Write-SuccessMessage "$Description started successfully!"
        Write-Host "   🕐 Trigger: $TriggerName" -ForegroundColor Yellow
        Write-Host "   🏭 Data Factory: $DataFactoryName" -ForegroundColor Yellow
        Write-Host "   📍 Resource Group: $ResourceGroupName" -ForegroundColor Yellow
    } else {
        Write-ErrorMessage "Failed to start $Description"
        exit 1
    }
}

function Stop-Trigger {
    param([string]$TriggerName, [string]$Description)
    
    Write-StatusMessage "Stopping $Description ($TriggerName)..."
    
    $result = az datafactory trigger stop `
        --resource-group $ResourceGroupName `
        --factory-name $DataFactoryName `
        --name $TriggerName `
        --output none
    
    if ($LASTEXITCODE -eq 0) {
        Write-SuccessMessage "$Description stopped successfully!"
        Write-Host "   🕐 Trigger: $TriggerName" -ForegroundColor Yellow
        Write-Host "   🏭 Data Factory: $DataFactoryName" -ForegroundColor Yellow
        Write-Host "   📍 Resource Group: $ResourceGroupName" -ForegroundColor Yellow
    } else {
        Write-ErrorMessage "Failed to stop $Description"
        exit 1
    }
}

# Main script logic
Write-Host "🚀 Azure Data Factory ETL Trigger Management" -ForegroundColor Magenta
Write-Host "=" * 60 -ForegroundColor Magenta
Write-Host "📅 $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Gray
Write-Host "🎯 Action: $Action" -ForegroundColor Cyan
Write-Host "🏭 Data Factory: $DataFactoryName" -ForegroundColor Cyan
Write-Host "📍 Resource Group: $ResourceGroupName" -ForegroundColor Cyan
Write-Host ""

switch ($Action) {
    "start-test" {
        Write-StatusMessage "🧪 STARTING TEST TRIGGER (10-minute intervals)"
        Start-Trigger $TestTriggerName "Test Trigger (10-minute intervals)"
        Write-Host ""
        Write-Host "⚠️  WARNING: Test trigger runs every 10 minutes!" -ForegroundColor Yellow
        Write-Host "   • Use 'stop-test' action to disable when testing is complete" -ForegroundColor Yellow
        Write-Host "   • Monitor costs and resource usage carefully" -ForegroundColor Yellow
    }
    
    "stop-test" {
        Write-StatusMessage "🛑 STOPPING TEST TRIGGER"
        Stop-Trigger $TestTriggerName "Test Trigger (10-minute intervals)"
        Write-Host ""
        Write-SuccessMessage "Test trigger disabled. ETL pipeline will not run automatically."
    }
    
    "start-daily" {
        Write-StatusMessage "📅 STARTING DAILY PRODUCTION TRIGGER (8:00 AM EST)"
        Start-Trigger $DailyTriggerName "Daily Production Trigger (8:00 AM EST)"
        Write-Host ""
        Write-Host "📊 PRODUCTION SCHEDULE ACTIVE:" -ForegroundColor Green
        Write-Host "   • Runs daily at 8:00 AM Eastern Time" -ForegroundColor Yellow
        Write-Host "   • Full ETL pipeline: Bronze → Silver → Gold" -ForegroundColor Yellow
        Write-Host "   • Automatic data quality validation" -ForegroundColor Yellow
    }
    
    "stop-daily" {
        Write-StatusMessage "🛑 STOPPING DAILY PRODUCTION TRIGGER"
        Stop-Trigger $DailyTriggerName "Daily Production Trigger (8:00 AM EST)"
        Write-Host ""
        Write-SuccessMessage "Daily production trigger disabled."
    }
    
    "switch-to-daily" {
        Write-StatusMessage "🔄 SWITCHING FROM TEST TO PRODUCTION SCHEDULE"
        Write-Host ""
        
        # Stop test trigger
        $testStatus = Get-TriggerStatus $TestTriggerName
        if ($testStatus -eq "Started") {
            Write-StatusMessage "Stopping test trigger..."
            Stop-Trigger $TestTriggerName "Test Trigger"
        } else {
            Write-StatusMessage "Test trigger is already stopped."
        }
        
        Write-Host ""
        
        # Start daily trigger
        Write-StatusMessage "Starting daily production trigger..."
        Start-Trigger $DailyTriggerName "Daily Production Trigger (8:00 AM EST)"
        
        Write-Host ""
        Write-SuccessMessage "🎉 SUCCESSFULLY SWITCHED TO PRODUCTION SCHEDULE!"
        Write-Host "   ❌ Test trigger (10-minute): DISABLED" -ForegroundColor Red
        Write-Host "   ✅ Daily trigger (8:00 AM EST): ENABLED" -ForegroundColor Green
    }
    
    "status" {
        Write-StatusMessage "📊 CHECKING TRIGGER STATUS"
        Write-Host ""
        
        $testStatus = Get-TriggerStatus $TestTriggerName
        $dailyStatus = Get-TriggerStatus $DailyTriggerName
        
        Write-Host "🧪 Test Trigger (10-minute intervals):" -ForegroundColor Cyan
        Write-Host "   • Name: $TestTriggerName" -ForegroundColor Gray
        Write-Host "   • Status: $testStatus" -ForegroundColor $(if ($testStatus -eq "Started") { "Green" } else { "Red" })
        Write-Host "   • Schedule: Every 10 minutes (EST)" -ForegroundColor Gray
        Write-Host ""
        
        Write-Host "📅 Daily Production Trigger:" -ForegroundColor Cyan
        Write-Host "   • Name: $DailyTriggerName" -ForegroundColor Gray
        Write-Host "   • Status: $dailyStatus" -ForegroundColor $(if ($dailyStatus -eq "Started") { "Green" } else { "Red" })
        Write-Host "   • Schedule: Daily at 8:00 AM EST" -ForegroundColor Gray
        Write-Host ""
        
        if ($testStatus -eq "Started" -and $dailyStatus -eq "Started") {
            Write-Host "⚠️  WARNING: Both triggers are active!" -ForegroundColor Yellow
            Write-Host "   This may cause overlapping pipeline runs and increased costs." -ForegroundColor Yellow
        } elseif ($testStatus -eq "Stopped" -and $dailyStatus -eq "Stopped") {
            Write-Host "ℹ️  No triggers are currently active." -ForegroundColor Blue
            Write-Host "   ETL pipeline will only run when triggered manually." -ForegroundColor Blue
        }
    }
    
    default {
        Write-ErrorMessage "Unknown action: $Action"
        exit 1
    }
}

Write-Host ""
Write-Host "=" * 60 -ForegroundColor Magenta
Write-Host "🎯 AVAILABLE ACTIONS:" -ForegroundColor Magenta
Write-Host "   • start-test      : Start 10-minute test trigger" -ForegroundColor Gray
Write-Host "   • stop-test       : Stop 10-minute test trigger" -ForegroundColor Gray
Write-Host "   • start-daily     : Start daily 8:00 AM trigger" -ForegroundColor Gray
Write-Host "   • stop-daily      : Stop daily 8:00 AM trigger" -ForegroundColor Gray
Write-Host "   • switch-to-daily : Stop test, start daily" -ForegroundColor Gray
Write-Host "   • status          : Check trigger status" -ForegroundColor Gray
Write-Host ""
Write-Host "💡 EXAMPLES:" -ForegroundColor Magenta
Write-Host "   .\Manage-ETL-Triggers.ps1 -Action start-test" -ForegroundColor Gray
Write-Host "   .\Manage-ETL-Triggers.ps1 -Action switch-to-daily" -ForegroundColor Gray
Write-Host "   .\Manage-ETL-Triggers.ps1 -Action status" -ForegroundColor Gray
Write-Host "=" * 60 -ForegroundColor Magenta
