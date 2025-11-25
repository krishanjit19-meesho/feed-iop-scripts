#!/usr/bin/env python3

"""
Script to compare two CSV files (feed_catalog_results.csv and query_catalog_results.csv)
and calculate match percentage for each query.
"""

import csv
import json

def calculate_match_percentage(l2_ids, feed_ids):
    """
    Calculate match percentage between two lists of catalog IDs.
    Uses Jaccard similarity: intersection / union
    """
    if not l2_ids and not feed_ids:
        return 100.0  # Both empty = 100% match
    
    if not l2_ids or not feed_ids:
        return 0.0  # One empty, one not = 0% match
    
    l2_set = set(l2_ids)
    feed_set = set(feed_ids)
    
    intersection = len(l2_set & feed_set)
    union = len(l2_set | feed_set)
    
    if union == 0:
        return 100.0
    
    match_percentage = (intersection / union) * 100
    return round(match_percentage, 2)

def compare_csv_files(l2_csv='query_catalog_results.csv', feed_csv='feed_catalog_results.csv', output_csv='match_percentage_results.csv'):
    """
    Compare two CSV files and calculate match percentage for each query.
    """
    results = []
    
    # Read L2 results
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
        print(f"  Found {len(l2_data)} queries in L2 results")
    except FileNotFoundError:
        print(f"Error: File {l2_csv} not found")
        return None
    
    # Read Feed results
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
        print(f"  Found {len(feed_data)} queries in Feed results")
    except FileNotFoundError:
        print(f"Error: File {feed_csv} not found")
        return None
    
    # Get all unique queries
    all_queries = set(l2_data.keys()) | set(feed_data.keys())
    print(f"\nComparing {len(all_queries)} queries...")
    
    # Compare each query
    for query in sorted(all_queries):
        l2_ids = l2_data.get(query, [])
        feed_ids = feed_data.get(query, [])
        
        match_percentage = calculate_match_percentage(l2_ids, feed_ids)
        
        results.append({
            'query': query,
            'match_percentage': match_percentage
        })
    
    # Write results to output CSV
    print(f"\nWriting results to {output_csv}...")
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['query', 'match_percentage'])
        writer.writeheader()
        writer.writerows(results)
    
    # Calculate summary statistics
    if results:
        percentages = [r['match_percentage'] for r in results]
        avg_percentage = sum(percentages) / len(percentages)
        perfect_matches = sum(1 for p in percentages if p == 100.0)
        zero_matches = sum(1 for p in percentages if p == 0.0)
        
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        print(f"Total queries: {len(results)}")
        print(f"Average match percentage: {avg_percentage:.2f}%")
        print(f"Perfect matches (100%): {perfect_matches} ({perfect_matches*100/len(results):.1f}%)")
        print(f"No matches (0%): {zero_matches} ({zero_matches*100/len(results):.1f}%)")
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

