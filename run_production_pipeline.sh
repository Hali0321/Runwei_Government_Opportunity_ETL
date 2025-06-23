#!/bin/bash

echo "🚀 Azure Grants.gov Production Pipeline"
echo "======================================"
echo "📅 Started: $(date)"

# Run Layer 2 Enhancement
echo ""
echo "🧹 Step 1: Running Layer 2 Enhancement..."
python3 production/scripts/layer2_enhancement.py

if [ $? -eq 0 ]; then
    echo "✅ Layer 2 completed successfully"
    
    # Run Layer 3 Selection
    echo ""
    echo "🎯 Step 2: Running Layer 3 Selection..."
    python3 production/scripts/layer3_selection.py
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "🎊 SUCCESS! Production pipeline completed!"
        echo "📊 Check dbo.FinalOpportunities for results"
    else
        echo "❌ Layer 3 failed"
        exit 1
    fi
else
    echo "❌ Layer 2 failed"
    exit 1
fi
