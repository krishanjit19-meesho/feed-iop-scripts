#!/usr/bin/env python3

"""
Simple script to fetch feed using gRPC.
Takes feed_id as input, gets QUL response, builds request, and calls gRPC.
"""

import json
import csv
import subprocess
import sys
from pathlib import Path
from qul_response import get_qul_response

# Configuration - all constants
FEED_CONTEXT = "text_search_mall_v1"
LIMIT = 10
USER_ID = "376902237"
USER_CITY = "bengaluru"
USER_STATE_CODE = "KA"
TENANT_CONTEXT = "organic"
SESSION_ID = "session_id"
ENTITY_ID = 123
GRPC_HOST = "localhost"
GRPC_PORT = 8080
FEED_ID = "farmley"
INPUT_FILE = 'temp_sorted.csv'
OUTPUT_FILE = 'feed_catalog_results.csv'
# Proto paths - Configure this to point to your feed-iop repository
#feed-iop repository path
PROTO_BASE_DIR = Path("/Users/krishanjitrajbongshi/Documents/Github/feed-iop")

# Option 2: Uncomment below to use relative path (if script is in feed-iop directory)
# PROTO_BASE_DIR = Path(__file__).parent

PROTO_DIR = PROTO_BASE_DIR / "client" / "proto" / "feed"
PROTO_FILE = "api.proto"


def build_feed_request(feed_id, preprocessor_response):
    """Build the feed request JSON with constant values."""
    return {
        "feed_request_context": {
            "feed_type": "text_search",
            "feed_context": FEED_CONTEXT,
            "feed_id": feed_id,
            "search_metadata": {
                "preprocessor_response": preprocessor_response
            }
        },
        "limit": LIMIT,
        "session_context": {
            "session_id": SESSION_ID
        },
        "tenant_request_context": {
            "tenant_context": TENANT_CONTEXT
        },
        "sort_config": {
            "sort_by": "most_relevant",
            "sort_order": "desc"
        },
        "filter_config": {
            "applied_filters": {
                "filter_list": []
            }
        }
    }


def call_grpc(request_json):
    """Make gRPC call using grpcurl."""
    headers = {
        "meesho-user-id": USER_ID,
        "meesho-user-context": "logged_in",
        "meesho-user-state-code": USER_STATE_CODE,
        "meesho-user-city": USER_CITY,
        "data_logging_enabled": "false",
        "tenant-context": TENANT_CONTEXT
    }
    
    # Check if proto directory exists
    if not PROTO_DIR.exists():
        raise Exception(f"Proto directory not found: {PROTO_DIR}")
    
    proto_file_path = PROTO_DIR / PROTO_FILE
    if not proto_file_path.exists():
        raise Exception(f"Proto file not found: {proto_file_path}")
    
    # Build grpcurl command
    # Format: grpcurl -plaintext -import-path <dir> -proto <file> -d @ -H <headers> <host:port> <method>
    cmd = ["grpcurl", "-plaintext"]
    
    # Add proto import path (pointing to the feed directory)
    cmd.extend(["-import-path", str(PROTO_DIR)])
    # Add proto file (just the filename since import-path points to the directory)
    cmd.extend(["-proto", PROTO_FILE])
    # Add -d @ to read from stdin
    cmd.extend(["-d", "@"])
    
    # Add headers
    for key, value in headers.items():
        cmd.extend(["-H", f"{key}:{value}"])
    
    # Add server and method
    cmd.append(f"{GRPC_HOST}:{GRPC_PORT}")
    cmd.append("feed.FeedIopService/FetchFeed")
    
    # Convert request JSON to string and pass directly via stdin
    request_json_str = json.dumps(request_json)
    
    # Make gRPC call - pass JSON directly via stdin
    result = subprocess.run(
        cmd,
        input=request_json_str,
        capture_output=True,
        text=True,
        check=False,
        cwd=str(PROTO_DIR)
    )
    
    if result.returncode != 0:
        error_msg = result.stderr or result.stdout
        # print(f"gRPC call failed: {error_msg}")
        raise Exception(f"gRPC call failed: {error_msg}")
    
    return json.loads(result.stdout)


def extract_catalog_ids(response):
    """Extract catalog IDs from response and convert to integers."""
    catalog_ids = []
    
    if isinstance(response, dict):
        if 'entities' in response:
            catalog_ids = [e.get('entity_id') or e.get('catalog_id') 
                          for e in response['entities'] 
                          if isinstance(e, dict)]
        elif 'catalogs' in response:
            catalog_ids = [c.get('catalog_id') 
                          for c in response['catalogs'] 
                          if isinstance(c, dict) and c.get('catalog_id')]
        elif 'data' in response:
            if 'catalogs' in response['data']:
                catalog_ids = [c.get('catalog_id') 
                              for c in response['data']['catalogs'] 
                              if isinstance(c, dict) and c.get('catalog_id')]
            elif 'entities' in response['data']:
                catalog_ids = [e.get('entity_id') or e.get('catalog_id') 
                              for e in response['data']['entities'] 
                              if isinstance(e, dict)]
            elif 'items' in response['data']:
                catalog_ids = [item.get('entityResponse', {}).get('catalogId') 
                              for item in response['data']['items'] 
                              if isinstance(item, dict) and item.get('entityResponse', {}).get('catalogId')]
    
    # Convert all catalog IDs to integers and filter out None values
    catalog_ids_int = []
    for cat_id in catalog_ids:
        if cat_id is not None:
            try:
                catalog_ids_int.append(int(cat_id))
            except (ValueError, TypeError):
                # Skip if conversion fails
                continue
    
    return catalog_ids_int


def fetch_feed_for_query(query):
    """
    Complete pipeline: Get QUL response and then fetch feed via gRPC
    """
    # print(f"Processing query: '{query}'")
    
    # Get QUL response
    # print("Getting QUL response...")
    qul_response = get_qul_response(query)
    if qul_response is None:
        # print("Failed to get QUL response")
        return None
    
    # print("QUL response received")
    
    # Convert QUL response to JSON string (formatted with newlines)
    preprocessor_response = json.dumps(qul_response, indent=4)
    
    # Build feed request
    request = build_feed_request(query, preprocessor_response)
    
    # Make gRPC call
    try:
        response = call_grpc(request)
        # print("✓ gRPC call successful")
        
        # Extract catalog IDs
        catalog_ids = extract_catalog_ids(response)
        return catalog_ids
    
    except Exception as e:
        # print(f"✗ Error: {e}")
        return None


def process_queries_from_csv(input_csv=INPUT_FILE, output_csv=OUTPUT_FILE, limit=5000):
    """
    Process queries from CSV file and write results to output CSV
    """
    results = []
    
    # Read queries from input CSV
    # print(f"Reading queries from {input_csv}...")
    try:
        with open(input_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            queries = [row['query'] for row in reader if row.get('query')]
        
        # Limit to first N queries
        queries = queries[:limit]
        print(f"Found {len(queries)} queries to process")
        
        success_count = 0
        failed_count = 0
        
        # Process each query
        for idx, query in enumerate(queries, 1):
            print(f"[{idx}/{len(queries)}] Processing: '{query}'", end=' ... ', flush=True)
            
            catalog_ids = fetch_feed_for_query(query)
            
            if catalog_ids:
                # Format catalog_ids as a list string
                catalog_ids_str = json.dumps(catalog_ids)
                success_count += 1
                print(f"✓ Success ({len(catalog_ids)} catalog IDs) | Total: {success_count} success, {failed_count} failed")
                results.append({
                    'query': query,
                    'catalog_id': catalog_ids_str
                })
            else:
                # Write a row for failed queries
                failed_count += 1
                print(f"✗ Failed | Total: {success_count} success, {failed_count} failed")
                results.append({
                    'query': query,
                    'catalog_id': '[]'
                })
        
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
    """Main function."""
    # Get feed_id from command line or use default
    feed_id = FEED_ID
    query = feed_id  # feed_id = query
    
    # print(f"Feed ID: {feed_id}")
    # print("Getting QUL response...")
    
    # Get QUL response
    qul_response = get_qul_response(query)
    if qul_response is None:
        print("Failed to get QUL response")
        sys.exit(1)
    
    
    # Convert QUL response to JSON string (formatted with newlines)
    preprocessor_response = json.dumps(qul_response, indent=4)
    
    # Build feed request dynamically (all in memory, no external file)
    request = build_feed_request(feed_id, preprocessor_response)
    
    # Make gRPC call
    print(f"Calling gRPC at {GRPC_HOST}:{GRPC_PORT}...")
    try:
        response = call_grpc(request)
        print("✓ gRPC call successful")
        
        # Extract and print catalog IDs
        catalog_ids = extract_catalog_ids(response)
        if catalog_ids:
            # print(f"\nCatalog IDs found ({len(catalog_ids)}):")
            print(catalog_ids)
        else:
            print("\nNo catalog IDs found in response")
            print("Response:", json.dumps(response, indent=2)[:500])
    
    except Exception as e:
        print(f"✗ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Check if CSV processing mode is requested
    if len(sys.argv) > 1 and sys.argv[1] == '--process-csv':
        input_file = sys.argv[2] if len(sys.argv) > 2 else INPUT_FILE
        output_file = sys.argv[3] if len(sys.argv) > 3 else OUTPUT_FILE
        limit = 500
        
        print("="*60)
        print("CSV Processing Mode")
        print(f"Processing first {limit} queries from {input_file}")
        print("="*60)
        process_queries_from_csv(input_file, output_file, limit)
    else:
        # Default: single query mode
        main()

