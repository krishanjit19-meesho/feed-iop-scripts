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
INPUT_FILE = 'temp.csv'
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
        "dag_name": DAG_NAME,
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
    Returns: (result_dict, status) where status is "OK" or "FAILED"
    """
    # Step 1: Get QUL response
    if test:
        qul_response = get_qul_response_test(query)
    else:
        qul_response = get_qul_response(query)
    
    if qul_response is None:
        return (None, "FAILED")
    
    # Step 2: Get L2 candidates using QUL response
    search_response = get_l2_candidates(query, qul_response)
    
    if search_response is None:
        return (None, "FAILED")
    
    return ({
        "query": query,
        "qul_response": qul_response,
        "search_response": search_response
    }, "OK")

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

def process_queries_from_csv(input_csv=INPUT_FILE, output_csv=OUTPUT_FILE, test=False, limit=None):
    """
    Process queries from CSV file and write results to output CSV with status column
    """
    results = []
    
    # Read queries from input CSV
    try:
        with open(input_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            queries = [row['query'] for row in reader if row.get('query')]
        
        # Apply limit if specified
        total_queries = len(queries)
        if limit is not None and limit > 0:
            queries = queries[:limit]
            print(f"Found {total_queries} queries in {input_csv}, processing first {len(queries)}")
        else:
            print(f"Found {len(queries)} queries in {input_csv}, processing all")
        
        print(f"Running L2 candidates tests...\n")
        
        success_count = 0
        failed_count = 0
        
        # Process each query
        for idx, query in enumerate(queries, 1):
            print(f"[{idx}/{len(queries)}] Processing: '{query}'", end=' ... ', flush=True)
            
            result, status = get_l2_candidates_with_qul_integration(query, test)
            
            if status == "OK" and result:
                search_response = result['search_response']
                catalog_ids, _ = extract_catalog_ids_and_dag_name(search_response)
                
                # Format catalog_ids as a list string
                if catalog_ids:
                    catalog_ids_str = json.dumps(catalog_ids)
                else:
                    catalog_ids_str = '[]'
                
                success_count += 1
                print(f"✓ OK ({len(catalog_ids)} catalog IDs) | Total: {success_count} OK, {failed_count} FAILED")
                results.append({
                    'query': query,
                    'catalog_id': catalog_ids_str,
                    'status': 'OK'
                })
            else:
                # Write a row for failed queries
                failed_count += 1
                print(f"✗ FAILED | Total: {success_count} OK, {failed_count} FAILED")
                results.append({
                    'query': query,
                    'catalog_id': '[]',
                    'status': 'FAILED'
                })
        
        # Write results to output CSV
        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['query', 'catalog_id', 'status'])
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\n✓ Successfully processed {len(queries)} queries and wrote {len(results)} results to {output_csv}")
        
        # Print summary
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print(f"Total queries tested: {len(queries)}")
        print(f"OK: {success_count}")
        print(f"FAILED: {failed_count}")
        if len(queries) > 0:
            print(f"Success rate: {(success_count/len(queries)*100):.2f}%")
        print("="*80)
        
        return output_csv
        
    except FileNotFoundError:
        print(f"✗ Error: File {input_csv} not found")
        return None
    except Exception as e:
        print(f"✗ Error processing CSV: {e}")
        return None

def retry_failed_queries(csv_file_path, test=False):
    """Rerun tests for all queries that have FAILED status in the CSV file.
    
    Args:
        csv_file_path: Path to the CSV file containing previous results
        test: Whether to use test mode for QUL response
    """
    # Read existing results from CSV
    existing_results = []
    failed_queries = []
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                query = row.get('query', '').strip()
                status = row.get('status', '').strip()
                catalog_id = row.get('catalog_id', '').strip()
                
                if query:
                    existing_results.append({
                        'query': query,
                        'catalog_id': catalog_id,
                        'status': status
                    })
                    
                    if status == 'FAILED':
                        failed_queries.append(query)
    except FileNotFoundError:
        print(f"✗ Error: CSV file not found: {csv_file_path}")
        print("Please run the script first to generate results.")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error reading CSV file: {e}")
        sys.exit(1)
    
    if not failed_queries:
        print(f"✓ No failed queries found in {csv_file_path}")
        print("All queries have passed or the file is empty.")
        return
    
    print(f"Found {len(failed_queries)} failed queries to retry")
    print(f"Running L2 candidates tests for failed queries...\n")
    
    # Create a mapping of query to its index in existing_results for quick updates
    query_to_index = {result['query']: idx for idx, result in enumerate(existing_results)}
    
    # Retry failed queries
    retried = 0
    passed_after_retry = 0
    still_failed = 0
    
    for idx, query in enumerate(failed_queries, 1):
        print(f"[{idx}/{len(failed_queries)}] Retrying query: {query}", end=' ... ', flush=True)
        
        result, status = get_l2_candidates_with_qul_integration(query, test)
        
        # Update the existing result
        result_idx = query_to_index[query]
        
        if status == "OK" and result:
            search_response = result['search_response']
            catalog_ids, _ = extract_catalog_ids_and_dag_name(search_response)
            
            if catalog_ids:
                catalog_ids_str = json.dumps(catalog_ids)
            else:
                catalog_ids_str = '[]'
            
            existing_results[result_idx]['catalog_id'] = catalog_ids_str
            existing_results[result_idx]['status'] = 'OK'
            passed_after_retry += 1
            print(f"✓ OK ({len(catalog_ids)} catalog IDs)")
        else:
            existing_results[result_idx]['catalog_id'] = '[]'
            existing_results[result_idx]['status'] = 'FAILED'
            still_failed += 1
            print(f"✗ FAILED")
        
        retried += 1
    
    # Write updated results back to CSV
    try:
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['query', 'catalog_id', 'status'])
            writer.writeheader()
            writer.writerows(existing_results)
        print(f"\n✓ Updated results written to {csv_file_path}")
    except Exception as e:
        print(f"✗ Error writing results CSV: {e}")
        sys.exit(1)
    
    # Print summary
    print("\n" + "="*80)
    print("RETRY SUMMARY")
    print("="*80)
    print(f"Total failed queries retried: {retried}")
    print(f"OK after retry: {passed_after_retry}")
    print(f"Still FAILED: {still_failed}")
    if retried > 0:
        print(f"Success rate after retry: {(passed_after_retry/retried*100):.2f}%")
    print("="*80)


def main():
    """
    Main function to test the complete pipeline
    """
    result, status = get_l2_candidates_with_qul_integration(QUERY, TEST)
    
    if status == "OK" and result:
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
    
    # Check if retry failed mode is requested
    if len(sys.argv) > 1 and sys.argv[1] == "--retry-failed":
        test_mode = '--test' in sys.argv
        retry_failed_queries(OUTPUT_FILE, test_mode)
    else:
        # Default: batch processing mode from CSV
        # Parse --limit flag
        limit = None
        if "--limit" in sys.argv:
            limit_idx = sys.argv.index("--limit")
            if limit_idx + 1 < len(sys.argv):
                try:
                    limit = int(sys.argv[limit_idx + 1])
                except ValueError:
                    print("✗ Error: --limit must be followed by a number")
                    sys.exit(1)
            else:
                print("✗ Error: --limit requires a number")
                sys.exit(1)
        
        test_mode = '--test' in sys.argv
        process_queries_from_csv(INPUT_FILE, OUTPUT_FILE, test_mode, limit)
