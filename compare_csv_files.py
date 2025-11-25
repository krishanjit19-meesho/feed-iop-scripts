#!/usr/bin/env python3

"""
Script to compare two CSV files (feed_catalog_results.csv and query_catalog_results.csv)
and check if catalog ID arrays match exactly for each query.

For each query:
- If arrays are equal → PASSED
- If arrays are not equal → FAILED
"""

import csv
import json

def compare_arrays(l2_ids, feed_ids):
    """
    Compare two arrays of catalog IDs and return status and failure reason.
    
    Args:
        l2_ids: List of catalog IDs from older flow (query_catalog_results.csv)
        feed_ids: List of catalog IDs from new flow (feed_catalog_results.csv)
    
    Returns:
        Tuple of (status, failure_reason) where:
        - status is "PASSED" or "FAILED"
        - failure_reason is empty string if PASSED, or description of failure
    """
    # Check if arrays are equal
    if l2_ids == feed_ids:
        return ("PASSED", "")
    
    # Arrays are different - determine the reason
    l2_len = len(l2_ids)
    feed_len = len(feed_ids)
    
    if l2_len != feed_len:
        return ("FAILED", f"Different lengths: older flow has {l2_len} items, new flow has {feed_len} items")
    
    # Same length but different values - find first difference
    min_len = min(l2_len, feed_len)
    for i in range(min_len):
        if l2_ids[i] != feed_ids[i]:
            return ("FAILED", f"Different value at position {i}: older flow has {l2_ids[i]}, new flow has {feed_ids[i]}")
    
    # Should not reach here, but just in case
    return ("FAILED", "Arrays differ but reason unclear")

def compare_csv_files(l2_csv='query_catalog_results.csv', feed_csv='feed_catalog_results.csv', output_csv='match_percentage_results.csv'):
    """
    Compare two CSV files and check if catalog ID arrays match exactly for each query.
    """
    results = []
    
    # Read L2 results (older flow)
    print(f"Reading {l2_csv}...")
    l2_data = {}
    try:
        with open(l2_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                query = row['query']
                catalog_ids_str = row.get('catalog_id', '[]')
                try:
                    catalog_ids = json.loads(catalog_ids_str)
                    l2_data[query] = catalog_ids
                except json.JSONDecodeError:
                    l2_data[query] = []
        print(f"  Found {len(l2_data)} queries in older flow results")
    except FileNotFoundError:
        print(f"✗ Error: File {l2_csv} not found")
        return None
    
    # Read Feed results (new flow)
    print(f"Reading {feed_csv}...")
    feed_data = {}
    try:
        with open(feed_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                query = row['query']
                catalog_ids_str = row.get('catalog_id', '[]')
                try:
                    catalog_ids = json.loads(catalog_ids_str)
                    feed_data[query] = catalog_ids
                except json.JSONDecodeError:
                    feed_data[query] = []
        print(f"  Found {len(feed_data)} queries in new flow results")
    except FileNotFoundError:
        print(f"✗ Error: File {feed_csv} not found")
        return None
    
    # Get all unique queries
    all_queries = set(l2_data.keys()) | set(feed_data.keys())
    print(f"\nComparing {len(all_queries)} queries...")
    
    # Compare each query
    for query in sorted(all_queries):
        l2_ids = l2_data.get(query, [])
        feed_ids = feed_data.get(query, [])
        
        status, failure_reason = compare_arrays(l2_ids, feed_ids)
        
        results.append({
            'query': query,
            'status': status,
            'failure_reason': failure_reason
        })
    
    # Write results to output CSV
    print(f"\nWriting results to {output_csv}...")
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['query', 'status', 'failure_reason'])
        writer.writeheader()
        writer.writerows(results)
    
    # Calculate summary statistics
    if results:
        passed_count = sum(1 for r in results if r['status'] == 'PASSED')
        failed_count = sum(1 for r in results if r['status'] == 'FAILED')
        total_count = len(results)
        passed_percentage = (passed_count / total_count * 100) if total_count > 0 else 0.0
        
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        print(f"Total queries: {total_count}")
        print(f"PASSED: {passed_count}")
        print(f"FAILED: {failed_count}")
        print(f"Pass percentage: {passed_percentage:.2f}%")
        print(f"{'='*60}")
        print(f"✓ Successfully wrote results to {output_csv}")
    
    return output_csv

if __name__ == "__main__":
    import sys
    
    l2_file = sys.argv[1] if len(sys.argv) > 1 else 'query_catalog_results.csv'
    feed_file = sys.argv[2] if len(sys.argv) > 2 else 'feed_catalog_results.csv'
    output_file = sys.argv[3] if len(sys.argv) > 3 else 'match_percentage_results.csv'
    
    print("="*60)
    print("CSV Comparison Tool")
    print("="*60)
    print()
    
    compare_csv_files(l2_file, feed_file, output_file)

