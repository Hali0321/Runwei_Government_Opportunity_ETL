#!/bin/bash

echo "🧪 TESTING COMPLETE AUTOMATION PIPELINE"
echo "======================================="
echo "📅 Test time: $(date)"

# Create test log
mkdir -p logs
TEST_LOG="logs/test_execution_$(date +%Y%m%d_%H%M%S).log"

echo "🎯 Running test of complete automation..."
echo "📊 Check $TEST_LOG for detailed output"

# Run the master automation controller
python3 production/scripts/master_automation_controller.py 2>&1 | tee "$TEST_LOG"

TEST_EXIT_CODE=$?

echo ""
echo "🏁 TEST RESULTS"
echo "==============="

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "✅ SUCCESS: Complete automation test passed!"
    echo "🎊 Your pipeline is ready for daily automation"
    echo ""
    echo "📊 NEXT STEPS:"
    echo "   1. Run: ./setup_automation.sh (to enable daily scheduling)"
    echo "   2. Check FinalOpportunities table for results"
    echo "   3. Integrate with your company's application"
else
    echo "❌ FAILURE: Automation test failed"
    echo "📋 Check the test log for troubleshooting: $TEST_LOG"
fi

echo ""
echo "📁 Test log saved: $TEST_LOG"

exit $TEST_EXIT_CODE
