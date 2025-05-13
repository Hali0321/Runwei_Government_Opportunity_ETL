import logging
import azure.functions as func
import requests
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('ApiTester HTTP trigger function processed a request.')
    
    # Get parameters
    endpoint = req.params.get('endpoint', 'search')
    keyword = req.params.get('keyword', 'education')
    grant_id = req.params.get('id', 'GR0010742')
    
    result = {
        "status": "unknown",
        "endpoint": endpoint,
        "response": {}
    }
    
    try:
        if endpoint.lower() == 'search':
            # Test the search API
            api_url = "https://api.grants.gov/v1/api/search2"
            payload = {"keyword": keyword, "rows": 5}
            headers = {"Content-Type": "application/json"}
            
            logging.info(f"Calling Search API: {api_url}")
            response = requests.post(api_url, headers=headers, json=payload)
            
        elif endpoint.lower() == 'detail':
            # Test the grant details API
            api_url = f"https://api.grants.gov/v1/api/fetchOpportunity/{grant_id}"
            headers = {"Content-Type": "application/json"}
            
            logging.info(f"Calling Details API: {api_url}")
            response = requests.get(api_url, headers=headers)
            
        else:
            return func.HttpResponse(
                json.dumps({"error": "Invalid endpoint. Use 'search' or 'detail'"}),
                mimetype="application/json",
                status_code=400
            )
        
        # Process response
        result["status"] = response.status_code
        
        # Try to parse as JSON
        try:
            result["response"] = response.json()
        except json.JSONDecodeError:
            result["response"] = {"text": response.text[:1000] + "..." if len(response.text) > 1000 else response.text}
            
        # Add headers info
        result["headers"] = dict(response.headers)
        
        return func.HttpResponse(
            json.dumps(result, indent=2),
            mimetype="application/json",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        
        return func.HttpResponse(
            json.dumps({"error": str(e), "traceback": traceback.format_exc()}),
            mimetype="application/json",
            status_code=500
        )
