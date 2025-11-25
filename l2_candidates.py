import requests
import json
import csv
import base64
import re
from qul_response import get_qul_response

# For test version, use the same function (can be modified later if needed)
def get_qul_response_test(query="cooker"):
    """Test version - currently same as control"""
    return get_qul_response(query)

TEST = False
LIMIT = 10
USER_ID = '39072829'
QUERY = "farmley"
TENANT_ID = "organic"
INPUT_FILE = 'temp_sorted.csv'
OUTPUT_FILE = 'query_catalog_results.csv'
DAG_NAME="organic_all_ss_sscat_route_cg"

def get_l2_candidates(query="cooker", qul_response_data=None):
    """
    Get L2 candidates from search orchestrator using QUL response
    """
    url = "http://search-orchestrator.prd.meesho.int/v1/search/text"
    
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'MEESHO-ISO-COUNTRY-CODE': 'IN',
        'MEESHO-CLIENT-ID': 'android',
        'Access-Token': '738a10520fb5219615358e85e6dee3cc479510f0',
        'RANKING-HOLDOUT-AUDIENCE': '0',
        'MEESHO-USER-CONTEXT': 'logged_in',
        'TENANT-ID': 'organic',
        'ENTITY-TYPE': 'catalog',
        'MEESHO-ISO-LANGUAGE-CODE': 'en',
        'MEESHO-USER-ID': USER_ID,
        'MEESHO-USER-STATE-CODE': 'UP',
        'SEARCH-TENANT-ID': TENANT_ID
    }
    
    # If qul_response_data is not provided, get it from QUL service
    if qul_response_data is None:
        # print("Failed to get QUL response")
        return None
    
    # Convert QUL response to JSON string for the search request
    qul_response_string = json.dumps(qul_response_data)
    # print("qul_response_string: ", qul_response_string)
    
    payload = {
        "dag_name": dag_name,
        "query": query,
        "limit": LIMIT,
        "sort_by": "most_relevant",
        "sort_order": "desc",
        "feed_params": {
            "search": {
                "search_id": None,
                "is_autocorrect_reverted": False
            }
        },
        "qul_response": qul_response_string,
        "applied_filters": []
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()  # Raise an exception for bad status codes
        # print(json.dumps(response.json(), indent=2)[:1000])
        
        return response.json()
    
    except requests.exceptions.RequestException as e:
        # print(f"Error making search request: {e}")
        return None
    except json.JSONDecodeError as e:
        # print(f"Error parsing search response JSON: {e}")
        return None

def get_l2_candidates_with_qul_integration(query="cooker", test=False):
    """
    Complete pipeline: Get QUL response and then search candidates
    """
    # print(f"Starting L2 candidates pipeline for query: '{query}'")
    
    # Step 1: Get QUL response
    if test:
        # print("Getting QUL response from test...")
        qul_response = get_qul_response_test(query)
    else:
        # print("Getting QUL response from control...")
        qul_response = get_qul_response(query)
    
    if qul_response is None:
        # print("Failed to get QUL response. Stopping pipeline.")
        return None
    
    # print("QUL response received successfully!")
    
    # Step 2: Get L2 candidates using QUL response
    # print("Step 2: Getting L2 candidates...")
    search_response = get_l2_candidates(query, qul_response)
    
    if search_response is None:
        # print("Failed to get L2 candidates.")
        return None
    
    # print("L2 candidates received successfully!")
    # print(search_response)
    
    return {
        "query": query,
        "qul_response": qul_response,
        "search_response": search_response
    }

def decode_cursor(cursor_string):
    """
    Decode cursor string (base64) and extract dag_name if available
    """
    if not cursor_string:
        return None
    
    try:
        # Try to decode base64
        decoded_bytes = base64.b64decode(cursor_string)
        decoded_str = decoded_bytes.decode('utf-8')
        
        # Try to parse as JSON
        try:
            cursor_data = json.loads(decoded_str)
            if isinstance(cursor_data, dict):
                return cursor_data.get('dag_name')
        except json.JSONDecodeError:
            # If not JSON, try to find dag_name in the string
            if 'dag_name' in decoded_str:
                # Try to extract dag_name value
                match = re.search(r'"dag_name"\s*:\s*"([^"]+)"', decoded_str)
                if match:
                    return match.group(1)
        
        return None
    except Exception as e:
        # print(f"Error decoding cursor: {e}")
        return None

def extract_catalog_ids_and_dag_name(search_response):
    """
    Extract catalog_ids and dag_name from search response
    Returns tuple: (catalog_ids list, dag_name or None)
    """
    # Check different possible response structures for orchestrator
    catalogs = []
    if 'catalogs' in search_response:
        catalogs = search_response['catalogs']
    elif 'data' in search_response and 'catalogs' in search_response['data']:
        catalogs = search_response['data']['catalogs']
    elif 'results' in search_response:
        catalogs = search_response['results']
    
    catalog_ids = []
    if catalogs:
        catalog_ids = [catalog.get('catalog_id') for catalog in catalogs if 'catalog_id' in catalog]
    
    # Try to extract cursor and decode dag_name
    dag_name = None
    cursor = None
    
    # Check for cursor in different possible locations
    if 'cursor' in search_response:
        cursor = search_response['cursor']
    elif 'data' in search_response and 'cursor' in search_response['data']:
        cursor = search_response['data']['cursor']
    elif 'pagination' in search_response and 'cursor' in search_response['pagination']:
        cursor = search_response['pagination']['cursor']
    
    if cursor:
        dag_name = decode_cursor(cursor)
    
    return catalog_ids, dag_name

def process_queries_from_csv(input_csv=INPUT_FILE, output_csv=OUTPUT_FILE, test=False, limit=100):
    
    results = []
    
    # Read queries from input CSV
    # print(f"Reading queries from {input_csv}...")
    try:
        with open(input_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            queries = [row['query'] for row in reader if row.get('query')]
        
        # Limit to first N queries
        queries = queries[:limit]
        # print(f"Found {len(queries)} queries to process")
        
        # Process each query
        for idx, query in enumerate(queries, 1):
            # print(f"\n[{idx}/{len(queries)}] Processing query: '{query}'")
            
            result = get_l2_candidates_with_qul_integration(query, test)
            
            if result:
                search_response = result['search_response']
                catalog_ids, _ = extract_catalog_ids_and_dag_name(search_response)
                
                # Format catalog_ids as a list string
                if catalog_ids:
                    catalog_ids_str = json.dumps(catalog_ids)
                else:
                    catalog_ids_str = '[]'
                
                results.append({
                    'query': query,
                    'catalog_id': catalog_ids_str
                })
                # print(f"  ✓ Found {len(catalog_ids)} catalog IDs")
            else:
                # Write a row for failed queries
                results.append({
                    'query': query,
                    'catalog_id': '[]'
                })
                # print(f"  ✗ Failed to get results")
        
        # Write results to output CSV
        # print(f"\nWriting {len(results)} results to {output_csv}...")
        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['query', 'catalog_id'])
            writer.writeheader()
            writer.writerows(results)
        
        print(f"✓ Successfully processed {len(queries)} queries and wrote {len(results)} results to {output_csv}")
        return output_csv
        
    except FileNotFoundError:
        print(f"Error: File {input_csv} not found")
        return None
    except Exception as e:
        print(f"Error processing CSV: {e}")
        return None

def main():
    """
    Main function to test the complete pipeline
    """
    result = get_l2_candidates_with_qul_integration(QUERY, TEST)
    
    if result:
        # Extract catalog_ids from the search response
        search_response = result['search_response']
        catalog_ids, _ = extract_catalog_ids_and_dag_name(search_response)
        
        if catalog_ids:
            # Print catalog_ids as an array
            print(f"\nCatalog IDs found ({len(catalog_ids)}):")
            print(catalog_ids)
        else:
            print("No catalogs found in search response")
            print("Response structure:", json.dumps(search_response, indent=2)[:500] + "...")
        
    else:
        print("Pipeline failed to complete successfully")

if __name__ == "__main__":
    import sys
    
    # Check if CSV processing mode is requested
    if len(sys.argv) > 1 and sys.argv[1] == '--process-csv':
        input_file = sys.argv[2] if len(sys.argv) > 2 else INPUT_FILE
        output_file = sys.argv[3] if len(sys.argv) > 3 else OUTPUT_FILE
        test_mode = '--test' in sys.argv
        limit = 500  # Process first 100 queries
        
        print("="*60)
        print("CSV Processing Mode")
        print(f"Processing first {limit} queries from {input_file}")
        print("="*60)
        process_queries_from_csv(input_file, output_file, test_mode, limit)
    else:
        # Default: single query mode
        main()
