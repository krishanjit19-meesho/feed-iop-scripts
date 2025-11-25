import requests
import json

def get_qul_response(query="cooker"):
    """
    Get response from QUL service using the provided configuration
    """
    url = "http://qul-web-internal.prd.meesho.int/internal/v2/preprocess/text"
    
    headers = {
        'Content-Type': 'application/json',
        'Access-Token': 'adsnsau7142bg7aawdjnawenawdaw8dna74bawe',
        'MEESHO-ISO-COUNTRY-CODE': 'IN'
    }
    
    payload = {
        "query": query,
        "normalization_config": {
            "enable": True,
            "version": 1,
            "async_process": True
        },
        "expansion_config": {
            "enable": True,
            "version": 1,
            "required": 3,
            "threshold": 0.01
        },
        "qcl_config": {
            "enable": True,
            "version": 2,
            "async_process": True,
            "sqcm_config": {
                "enable": True,
                "version": 1,
                "sqcm_prob": 0.8,
                "vol_threshold": 1000
            }
        },
        "query_tagging_config": {
            "qcmct": 0.7,
            "qtmct": 0.0,
            "variant": 2.0,
            "enable": True,
            "version": 1,
            "model_url": "",
            "qta_model_url": ""
        },
        "intent_detection_config": {
            "enable": True,
            "version": 3
        },
        "query_blacklist_config": {
            "enable": True
        },
        "async_process": True,
        "query_attribute_dictionary_config": {
            "enable": True,
            "version": 1
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        return None

def main():
    """
    Main function to test the QUL API call
    """
    print("Making request to QUL service...")
    
    # Get response for "cooker" query
    result = get_qul_response("artificial ashoka mala")
    
    if result:
        print("Response received successfully!")
        print(json.dumps(result, indent=2))
    else:
        print("Failed to get response from QUL service")

if __name__ == "__main__":
    main()
