#!/bin/bash

# Configuration
ACCOUNT_NAME="grantsgov58225"
TABLE_NAME="GrantDetails"
MAX_RETRIES=5
INITIAL_BACKOFF=2
BACKOFF_FACTOR=1.5

# Function to update a single grant with backoff
update_grant() {
    local grant_id=$1
    local retry=0
    local success=false
    
    while [ $retry -lt $MAX_RETRIES ] && [ "$success" = false ]; do
        echo "Attempting to update grant $grant_id (attempt $((retry+1)))"
        
        # Get grant data from API (simulated here)
        response=$(curl -s -o /dev/null -w "%{http_code}" "https://www.grants.gov/grantsws/rest/opportunities/opportunity/$grant_id")
        
        if [ "$response" = "200" ]; then
            # Simulated update (replace with actual update command)
            az storage entity merge \
              --table-name "$TABLE_NAME" \
              --account-name "$ACCOUNT_NAME" \
              --partition-key "Grant" \
              --row-key "$grant_id" \
              --entity AwardCeiling='{"$": "500000.0", "type": "Double"}' \
                       AwardFloor='{"$": "50000.0", "type": "Double"}'
            
            success=true
            echo "Successfully updated grant $grant_id"
        elif [ "$response" = "403" ]; then
            # Calculate backoff time
            backoff=$(echo "$INITIAL_BACKOFF * ($BACKOFF_FACTOR ^ $retry)" | bc)
            jitter=$(echo "scale=2; $RANDOM / 32767 * $backoff * 0.2" | bc)
            wait_time=$(echo "$backoff + $jitter" | bc)
            
            echo "Rate limited (status 403). Retrying in $wait_time seconds..."
            sleep $wait_time
            retry=$((retry+1))
        else
            echo "Error getting grant data: HTTP $response"
            retry=$((retry+1))
            sleep 1
        fi
    done
    
    if [ "$success" = false ]; then
        echo "Failed to update grant $grant_id after $MAX_RETRIES retries"
        return 1
    fi
    
    return 0
}

# Main script
grants_to_fix=(330934 275150)
success_count=0
error_count=0

for grant_id in "${grants_to_fix[@]}"; do
    if update_grant "$grant_id"; then
        success_count=$((success_count+1))
    else
        error_count=$((error_count+1))
    fi
    
    # Add delay between grant processing
    sleep 2
done

echo "Completed processing. Grants fixed: $success_count, errors: $error_count"
