#!/usr/bin/env python3

"""
Simple script to fetch feed using gRPC.
Takes feed_id as input, gets QUL response, builds request, and calls gRPC.
"""

import json
import subprocess
import sys
import tempfile
import os
import csv
from pathlib import Path
from qul_response import get_qul_response

# Configuration - all constants
FEED_CONTEXT = "text_search_mall_v1"
LIMIT = 10  # Default limit
TEST_LIMIT = 20  # Limit for pagination test
USER_ID = "376902237"
USER_CITY = "bengaluru"
USER_STATE_CODE = "KA"
TENANT_CONTEXT = "organic"
SESSION_ID = "session_id"
ENTITY_ID = 123
GRPC_HOST = "localhost"
GRPC_PORT = 8080

# Proto paths - Configure this to point to your feed-iop repository
# Option 1: Set absolute path (recommended if script is in different location)
PROTO_BASE_DIR = Path("/Users/krishanjitrajbongshi/Documents/Github/feed-iop")

# Option 2: Uncomment below to use relative path (if script is in feed-iop directory)
# PROTO_BASE_DIR = Path(__file__).parent

PROTO_DIR = PROTO_BASE_DIR / "client" / "proto" / "feed"
PROTO_FILE = "api.proto"


def build_feed_request(feed_id, preprocessor_response, cursor=None, limit=None):
    """Build the feed request JSON with constant values.
    
    Args:
        feed_id: The feed identifier (query/search term)
        preprocessor_response: The QUL preprocessor response JSON string
        cursor: Optional cursor for pagination (use cursor from last item of previous page)
        limit: Optional limit (defaults to LIMIT constant)
    """
    request_limit = limit if limit is not None else LIMIT
    request = {
        "feed_request_context": {
            "feed_type": "text_search",
            "feed_context": FEED_CONTEXT,
            "feed_id": feed_id,
            "search_metadata": {
                "preprocessor_response": preprocessor_response
            }
        },
        "limit": request_limit,
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
    
    # Add cursor if provided (for pagination)
    if cursor:
        request["cursor"] = cursor
    
    return request


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
    
    # Create temp file for request
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(request_json, f, indent=2)
        temp_file = f.name
    
    try:
        # Read from file and pipe to grpcurl
        # Use proto directory as working directory for grpcurl
        with open(temp_file, 'r') as f:
            result = subprocess.run(
                cmd,
                stdin=f,
                capture_output=True,
                text=True,
                check=False,
                cwd=str(PROTO_DIR)
            )
        
        if result.returncode != 0:
            error_msg = result.stderr or result.stdout
            raise Exception(f"gRPC call failed: {error_msg}")
        
        return json.loads(result.stdout)
    
    finally:
        if os.path.exists(temp_file):
            os.unlink(temp_file)


def extract_catalog_ids(response):
    """Extract catalog IDs from response."""
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
    
    return catalog_ids


def get_items(response):
    """Extract items from response.
    
    Args:
        response: The gRPC response dictionary
        
    Returns:
        List of items, or empty list if not found
    """
    if not isinstance(response, dict):
        return []
    
    if 'data' in response and 'items' in response['data']:
        return response['data']['items']
    elif 'items' in response:
        return response['items']
    
    return []


def get_last_cursor(response):
    """Extract the cursor from the last item in the response for pagination.
    
    Args:
        response: The gRPC response dictionary
        
    Returns:
        The cursor string from the last item, or None if no items found
    """
    items = get_items(response)
    if items and len(items) > 0:
        last_item = items[-1]
        # Handle both camelCase and snake_case field names
        cursor = last_item.get('cursor') or last_item.get('Cursor')
        return cursor
    
    return None


def get_second_last_cursor(response):
    """Extract the cursor from the second-last item in the response for pagination.
    
    Args:
        response: The gRPC response dictionary
        
    Returns:
        The cursor string from the second-last item, or None if not enough items found
    """
    items = get_items(response)
    if items and len(items) >= 2:
        second_last_item = items[-2]
        # Handle both camelCase and snake_case field names
        cursor = second_last_item.get('cursor') or second_last_item.get('Cursor')
        return cursor
    
    return None


def get_item_identifier(item):
    """Extract catalog_id and product_id from an item for comparison.
    
    Args:
        item: An item dictionary from the response
        
    Returns:
        Tuple of (catalog_id, product_id) or (None, None) if not found
    """
    entity_response = item.get('entity_response') or item.get('entityResponse') or {}
    catalog_id = entity_response.get('catalog_id') or entity_response.get('catalogId')
    product_id = entity_response.get('product_id') or entity_response.get('productId')
    return (catalog_id, product_id)


def compare_items(item1, item2):
    """Compare two items by their catalog_id and product_id.
    
    Args:
        item1: First item dictionary
        item2: Second item dictionary
        
    Returns:
        True if items match (same catalog_id and product_id), False otherwise
    """
    id1 = get_item_identifier(item1)
    id2 = get_item_identifier(item2)
    return id1 == id2 and id1[0] is not None


def main():
    """Main function."""
    # Get feed_id from command line or use default
    feed_id = sys.argv[1] if len(sys.argv) > 1 else "ml a1 mobile"
    # Optional: Get cursor from command line for pagination (2nd argument)
    cursor = sys.argv[2] if len(sys.argv) > 2 else None
    query = feed_id  # feed_id = query
    
    print(f"Feed ID: {feed_id}")
    if cursor:
        print(f"Using cursor for pagination: {cursor[:50]}...")
    print("Getting QUL response...")
    
    # Get QUL response
    qul_response = get_qul_response(query)
    if qul_response is None:
        print("Failed to get QUL response")
        sys.exit(1)
    
    print("QUL response received")
    
    # Convert QUL response to JSON string (formatted with newlines)
    preprocessor_response = json.dumps(qul_response, indent=4)
    
    # Build feed request dynamically
    request = build_feed_request(feed_id, preprocessor_response, cursor=cursor)
    
    # Create feed_request.json dynamically (always overwrites existing file)
    feed_request_file = "feed_request.json"
    with open(feed_request_file, 'w') as f:
        json.dump(request, f, indent=2)
    print(f"✓ Created {feed_request_file} dynamically with feed_id: {feed_id}")
    
    # Print the JSON file before gRPC call
    print("\n" + "="*60)
    print("Request JSON (feed_request.json):")
    print("="*60)
    print(json.dumps(request, indent=2))
    print("="*60 + "\n")
    
    # Make gRPC call
    print(f"Calling gRPC at {GRPC_HOST}:{GRPC_PORT}...")
    try:
        response = call_grpc(request)
        print("✓ gRPC call successful")
        
        # Extract and print catalog IDs
        catalog_ids = extract_catalog_ids(response)
        if catalog_ids:
            print(f"\nCatalog IDs found ({len(catalog_ids)}):")
            print(catalog_ids)
        else:
            print("\nNo catalog IDs found in response")
            print("Response:", json.dumps(response, indent=2)[:500])
        
        # Extract and display cursor for next page
        last_cursor = get_last_cursor(response)
        if last_cursor:
            print("\n" + "="*60)
            print("PAGINATION INFO:")
            print("="*60)
            print(f"Last item cursor (use this for next page):")
            print(f"{last_cursor}")
            print("\nTo fetch next page, run:")
            print(f"  python {sys.argv[0]} '{feed_id}' '{last_cursor}'")
            print("="*60)
        else:
            print("\nNo cursor found in response - this might be the last page")
    
    except Exception as e:
        print(f"✗ Error: {e}")
        sys.exit(1)


def test_pagination_continuity(feed_id, verbose=True):
    """Test pagination continuity using second-last cursor across 3 pages.
    
    Test case:
    1. Make first call (page 1) with limit 20
    2. Use cursor from second-last (19th) item to make page 2 call
    3. Verify that the last item from page 1 matches the first item from page 2
    4. Use cursor from second-last (19th) item of page 2 to make page 3 call
    5. Verify that the last item from page 2 matches the first item from page 3
    
    Args:
        feed_id: The feed identifier (query/search term)
        verbose: If True, print detailed output. If False, suppress most output.
        
    Returns:
        Tuple of (success: bool, error_message: str or None)
    """
    if verbose:
        print("="*80)
        print("PAGINATION CONTINUITY TEST (3 Pages, Limit 20)")
        print("="*80)
        print(f"Feed ID: {feed_id}")
        print(f"Limit per page: {TEST_LIMIT}\n")
    
    # Get QUL response
    if verbose:
        print("Step 1: Getting QUL response...")
    qul_response = get_qul_response(feed_id)
    if qul_response is None:
        error_msg = "Failed to get QUL response"
        if verbose:
            print(f"✗ {error_msg}")
        return (False, error_msg)
    if verbose:
        print("✓ QUL response received\n")
    
    preprocessor_response = json.dumps(qul_response, indent=4)
    
    # ========== PAGE 1 ==========
    if verbose:
        print("="*80)
        print("PAGE 1: Making first page request (no cursor)...")
        print("="*80)
    request1 = build_feed_request(feed_id, preprocessor_response, cursor=None, limit=TEST_LIMIT)
    
    try:
        response1 = call_grpc(request1)
        if verbose:
            print("✓ Page 1 call successful\n")
    except Exception as e:
        error_msg = f"Page 1 call failed: {e}"
        if verbose:
            print(f"✗ {error_msg}")
        return (False, error_msg)
    
    items1 = get_items(response1)
    if len(items1) < 2:
        error_msg = f"Not enough items in page 1 (got {len(items1)}, need at least 2)"
        if verbose:
            print(f"✗ {error_msg}")
        return (False, error_msg)
    
    if verbose:
        print(f"Page 1 returned {len(items1)} items")
    last_item_page1 = items1[-1]
    second_last_cursor_page1 = get_second_last_cursor(response1)
    
    if not second_last_cursor_page1:
        error_msg = "Could not extract cursor from second-last item in page 1"
        if verbose:
            print(f"✗ {error_msg}")
        return (False, error_msg)
    
    last_id_page1 = get_item_identifier(last_item_page1)
    if verbose:
        print(f"\nPage 1 - Last item (item {len(items1)}):")
        print(f"  Catalog ID: {last_id_page1[0]}, Product ID: {last_id_page1[1]}")
        print(f"Page 1 - Second-last cursor: {second_last_cursor_page1[:60]}...")
    
    # ========== PAGE 2 ==========
    if verbose:
        print("\n" + "="*80)
        print("PAGE 2: Making second page request (using second-last cursor from page 1)...")
        print("="*80)
    request2 = build_feed_request(feed_id, preprocessor_response, cursor=second_last_cursor_page1, limit=TEST_LIMIT)
    
    try:
        response2 = call_grpc(request2)
        if verbose:
            print("✓ Page 2 call successful\n")
    except Exception as e:
        error_msg = f"Page 2 call failed: {e}"
        if verbose:
            print(f"✗ {error_msg}")
        return (False, error_msg)
    
    items2 = get_items(response2)
    if len(items2) < 2:
        error_msg = f"Not enough items in page 2 (got {len(items2)}, need at least 2)"
        if verbose:
            print(f"✗ {error_msg}")
        return (False, error_msg)
    
    if verbose:
        print(f"Page 2 returned {len(items2)} items")
    first_item_page2 = items2[0]
    last_item_page2 = items2[-1]
    second_last_cursor_page2 = get_second_last_cursor(response2)
    
    if not second_last_cursor_page2:
        error_msg = "Could not extract cursor from second-last item in page 2"
        if verbose:
            print(f"✗ {error_msg}")
        return (False, error_msg)
    
    first_id_page2 = get_item_identifier(first_item_page2)
    last_id_page2 = get_item_identifier(last_item_page2)
    if verbose:
        print(f"\nPage 2 - First item (item 1):")
        print(f"  Catalog ID: {first_id_page2[0]}, Product ID: {first_id_page2[1]}")
        print(f"Page 2 - Last item (item {len(items2)}):")
        print(f"  Catalog ID: {last_id_page2[0]}, Product ID: {last_id_page2[1]}")
        print(f"Page 2 - Second-last cursor: {second_last_cursor_page2[:60]}...")
    
    # ========== PAGE 3 ==========
    if verbose:
        print("\n" + "="*80)
        print("PAGE 3: Making third page request (using second-last cursor from page 2)...")
        print("="*80)
    request3 = build_feed_request(feed_id, preprocessor_response, cursor=second_last_cursor_page2, limit=TEST_LIMIT)
    
    try:
        response3 = call_grpc(request3)
        if verbose:
            print("✓ Page 3 call successful\n")
    except Exception as e:
        error_msg = f"Page 3 call failed: {e}"
        if verbose:
            print(f"✗ {error_msg}")
        return (False, error_msg)
    
    items3 = get_items(response3)
    if len(items3) == 0:
        error_msg = "No items returned in page 3"
        if verbose:
            print(f"✗ {error_msg}")
        return (False, error_msg)
    
    if verbose:
        print(f"Page 3 returned {len(items3)} items")
    first_item_page3 = items3[0]
    first_id_page3 = get_item_identifier(first_item_page3)
    
    if verbose:
        print(f"\nPage 3 - First item (item 1):")
        print(f"  Catalog ID: {first_id_page3[0]}, Product ID: {first_id_page3[1]}")
    
    # ========== VERIFY CONTINUITY ==========
    if verbose:
        print("\n" + "="*80)
        print("VERIFYING PAGINATION CONTINUITY")
        print("="*80)
    
    all_tests_passed = True
    error_details = []
    
    # Test 1: Page 1 last item should match Page 2 first item
    if verbose:
        print("\nTest 1: Page 1 last item == Page 2 first item")
    if compare_items(last_item_page1, first_item_page2):
        if verbose:
            print("  ✓ PASSED: Last item from page 1 matches first item from page 2")
    else:
        error_detail = f"Page 1 last (Catalog ID={last_id_page1[0]}, Product ID={last_id_page1[1]}) != Page 2 first (Catalog ID={first_id_page2[0]}, Product ID={first_id_page2[1]})"
        error_details.append(error_detail)
        if verbose:
            print(f"  ✗ FAILED: Last item from page 1 does NOT match first item from page 2")
            print(f"    {error_detail}")
        all_tests_passed = False
    
    # Test 2: Page 2 last item should match Page 3 first item
    if verbose:
        print("\nTest 2: Page 2 last item == Page 3 first item")
    if compare_items(last_item_page2, first_item_page3):
        if verbose:
            print("  ✓ PASSED: Last item from page 2 matches first item from page 3")
    else:
        error_detail = f"Page 2 last (Catalog ID={last_id_page2[0]}, Product ID={last_id_page2[1]}) != Page 3 first (Catalog ID={first_id_page3[0]}, Product ID={first_id_page3[1]})"
        error_details.append(error_detail)
        if verbose:
            print(f"  ✗ FAILED: Last item from page 2 does NOT match first item from page 3")
            print(f"    {error_detail}")
        all_tests_passed = False
    
    # Final result
    if verbose:
        print("\n" + "="*80)
        if all_tests_passed:
            print("✓ ALL TESTS PASSED: Pagination continuity is correct across all 3 pages!")
            print("="*80)
        else:
            print("✗ SOME TESTS FAILED: Pagination continuity issues detected")
            print("="*80)
    
    if all_tests_passed:
        return (True, None)
    else:
        return (False, "; ".join(error_details))


def batch_test_from_csv(csv_file_path, output_csv_path, limit=None):
    """Read queries from CSV file and run pagination continuity test for each.
    
    Args:
        csv_file_path: Path to input CSV file containing queries
        output_csv_path: Path to output CSV file for results
        limit: Maximum number of queries to process (None for all queries)
    """
    # Read queries from CSV
    queries = []
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                query = row.get('query', '').strip()
                if query:
                    queries.append(query)
    except Exception as e:
        print(f"✗ Error reading CSV file: {e}")
        sys.exit(1)
    
    # Apply limit if specified
    total_queries = len(queries)
    if limit is not None and limit > 0:
        queries = queries[:limit]
        print(f"Found {total_queries} queries in {csv_file_path}, processing first {len(queries)}")
    else:
        print(f"Found {len(queries)} queries in {csv_file_path}, processing all")
    
    print(f"Running pagination continuity tests...\n")
    
    # Prepare results
    results = []
    total = len(queries)
    passed = 0
    failed = 0
    
    # Process each query
    for idx, query in enumerate(queries, 1):
        print(f"[{idx}/{total}] Testing query: {query}")
        try:
            success, error_msg = test_pagination_continuity(query, verbose=False)
            if success:
                status = "PASSED"
                passed += 1
                error_msg = None
            else:
                status = "FAILED"
                failed += 1
        except Exception as e:
            status = "FAILED"
            error_msg = f"Exception: {str(e)}"
            failed += 1
        
        results.append({
            'query': query,
            'status': status,
            'error_message': error_msg or ''
        })
        
        print(f"  Result: {status}")
        if error_msg:
            print(f"  Error: {error_msg[:100]}...")
        print()
    
    # Write results to CSV
    try:
        with open(output_csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['query', 'status', 'error_message'])
            writer.writeheader()
            writer.writerows(results)
        print(f"✓ Results written to {output_csv_path}")
    except Exception as e:
        print(f"✗ Error writing results CSV: {e}")
        sys.exit(1)
    
    # Print summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total queries tested: {total}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Success rate: {(passed/total*100):.2f}%")
    print("="*80)


def retry_failed_queries(csv_file_path):
    """Rerun tests for all queries that have FAILED status in the CSV file.
    
    Args:
        csv_file_path: Path to the CSV file containing previous results
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
                error_message = row.get('error_message', '').strip()
                
                if query:
                    existing_results.append({
                        'query': query,
                        'status': status,
                        'error_message': error_message
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
    print(f"Running pagination continuity tests for failed queries...\n")
    
    # Create a mapping of query to its index in existing_results for quick updates
    query_to_index = {result['query']: idx for idx, result in enumerate(existing_results)}
    
    # Retry failed queries
    retried = 0
    passed_after_retry = 0
    still_failed = 0
    
    for idx, query in enumerate(failed_queries, 1):
        print(f"[{idx}/{len(failed_queries)}] Retrying query: {query}")
        try:
            success, error_msg = test_pagination_continuity(query, verbose=False)
            if success:
                new_status = "PASSED"
                new_error_msg = ""
                passed_after_retry += 1
            else:
                new_status = "FAILED"
                new_error_msg = error_msg or ""
                still_failed += 1
        except Exception as e:
            new_status = "FAILED"
            new_error_msg = f"Exception: {str(e)}"
            still_failed += 1
        
        # Update the existing result
        result_idx = query_to_index[query]
        existing_results[result_idx]['status'] = new_status
        existing_results[result_idx]['error_message'] = new_error_msg
        
        print(f"  Result: {new_status}")
        if new_error_msg:
            print(f"  Error: {new_error_msg[:100]}...")
        print()
        retried += 1
    
    # Write updated results back to CSV
    try:
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['query', 'status', 'error_message'])
            writer.writeheader()
            writer.writerows(existing_results)
        print(f"✓ Updated results written to {csv_file_path}")
    except Exception as e:
        print(f"✗ Error writing results CSV: {e}")
        sys.exit(1)
    
    # Print summary
    print("\n" + "="*80)
    print("RETRY SUMMARY")
    print("="*80)
    print(f"Total failed queries retried: {retried}")
    print(f"Passed after retry: {passed_after_retry}")
    print(f"Still failed: {still_failed}")
    if retried > 0:
        print(f"Success rate after retry: {(passed_after_retry/retried*100):.2f}%")
    print("="*80)


def retry_grpc_failed_queries(csv_file_path):
    """Rerun tests only for queries that failed due to gRPC call errors.
    
    This function filters failed queries by checking if the error message
    contains "gRPC call failed" or "call failed" (indicating gRPC errors).
    
    Args:
        csv_file_path: Path to the CSV file containing previous results
    """
    # Read existing results from CSV
    existing_results = []
    grpc_failed_queries = []
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                query = row.get('query', '').strip()
                status = row.get('status', '').strip()
                error_message = row.get('error_message', '').strip()
                
                if query:
                    existing_results.append({
                        'query': query,
                        'status': status,
                        'error_message': error_message
                    })
                    
                    # Check if this is a gRPC call failure
                    if status == 'FAILED' and error_message:
                        error_lower = error_message.lower()
                        # Check for gRPC call errors
                        if 'grpc call failed' in error_lower or 'call failed' in error_lower:
                            grpc_failed_queries.append((query, error_message))
    except FileNotFoundError:
        print(f"✗ Error: CSV file not found: {csv_file_path}")
        print("Please run the script first to generate results.")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error reading CSV file: {e}")
        sys.exit(1)
    
    if not grpc_failed_queries:
        print(f"✓ No gRPC call failed queries found in {csv_file_path}")
        print("All gRPC calls succeeded or no failed queries with gRPC errors.")
        return
    
    print(f"Found {len(grpc_failed_queries)} queries that failed due to gRPC call errors")
    print(f"Running pagination continuity tests for gRPC-failed queries...\n")
    
    # Create a mapping of query to its index in existing_results for quick updates
    query_to_index = {result['query']: idx for idx, result in enumerate(existing_results)}
    
    # Retry gRPC-failed queries
    retried = 0
    passed_after_retry = 0
    still_failed = 0
    
    for idx, (query, original_error) in enumerate(grpc_failed_queries, 1):
        print(f"[{idx}/{len(grpc_failed_queries)}] Retrying query (gRPC error): {query}")
        print(f"  Original error: {original_error[:80]}...")
        try:
            success, error_msg = test_pagination_continuity(query, verbose=False)
            if success:
                new_status = "PASSED"
                new_error_msg = ""
                passed_after_retry += 1
            else:
                new_status = "FAILED"
                new_error_msg = error_msg or ""
                still_failed += 1
        except Exception as e:
            new_status = "FAILED"
            new_error_msg = f"Exception: {str(e)}"
            still_failed += 1
        
        # Update the existing result
        result_idx = query_to_index[query]
        existing_results[result_idx]['status'] = new_status
        existing_results[result_idx]['error_message'] = new_error_msg
        
        print(f"  Result: {new_status}")
        if new_error_msg:
            print(f"  New error: {new_error_msg[:100]}...")
        print()
        retried += 1
    
    # Write updated results back to CSV
    try:
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['query', 'status', 'error_message'])
            writer.writeheader()
            writer.writerows(existing_results)
        print(f"✓ Updated results written to {csv_file_path}")
    except Exception as e:
        print(f"✗ Error writing results CSV: {e}")
        sys.exit(1)
    
    # Print summary
    print("\n" + "="*80)
    print("gRPC RETRY SUMMARY")
    print("="*80)
    print(f"Total gRPC-failed queries retried: {retried}")
    print(f"Passed after retry: {passed_after_retry}")
    print(f"Still failed: {still_failed}")
    if retried > 0:
        print(f"Success rate after retry: {(passed_after_retry/retried*100):.2f}%")
    print("="*80)


if __name__ == "__main__":
    # Hardcoded input and output files
    INPUT_FILE = "temp.csv"
    OUTPUT_FILE = "pagination_result.csv"
    
    # Check if retry gRPC failed mode is requested
    if len(sys.argv) > 1 and sys.argv[1] == "--retry-grpc-failed":
        retry_grpc_failed_queries(OUTPUT_FILE)
    # Check if retry failed mode is requested
    elif len(sys.argv) > 1 and sys.argv[1] == "--retry-failed":
        retry_failed_queries(OUTPUT_FILE)
    # Check if single test mode is requested
    elif len(sys.argv) > 1 and sys.argv[1] == "--test":
        if len(sys.argv) < 3:
            print("✗ Error: --test requires a query as argument")
            print("Usage: python fetch_feed2.py --test \"your query\"")
            sys.exit(1)
        feed_id = sys.argv[2]
        success, error_msg = test_pagination_continuity(feed_id)
        if not success:
            sys.exit(1)
    else:
        # Default: batch test mode from CSV
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
        
        batch_test_from_csv(INPUT_FILE, OUTPUT_FILE, limit)

