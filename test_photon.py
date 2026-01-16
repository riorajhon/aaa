import requests
import json
import urllib3

# Disable SSL warnings if needed
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def test_photon_search():
    """Test Photon search API"""
    print("=" * 80)
    print("Testing Photon Search API")
    print("=" * 80)
    
    url = "https://photon.komoot.io/api/"
    query = "سقطرى"
    params = {
        'q': query
    }
    
    print(f"\nURL: {url}")
    print(f"Query: {query}")
    print(f"Params: {params}")
    print("\nTest 1: No headers")
    
    try:
        response = requests.get(url, params=params, timeout=10, verify=True)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Success! Features found: {len(data.get('features', []))}")
            
            if data.get('features'):
                feature = data['features'][0]
                print(f"\nFirst result:")
                print(json.dumps(feature, indent=2, ensure_ascii=False))
        else:
            print(f"✗ Failed with status {response.status_code}")
            print(f"Response: {response.text[:200]}")
    
    except Exception as e:
        print(f"✗ Error: {type(e).__name__}: {e}")
    
    # Test 2: With verify=False
    print("\n" + "-" * 80)
    print("Test 2: With verify=False (ignore SSL)")
    
    try:
        response = requests.get(url, params=params, timeout=10, verify=False)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Success! Features found: {len(data.get('features', []))}")
        else:
            print(f"✗ Failed with status {response.status_code}")
    
    except Exception as e:
        print(f"✗ Error: {type(e).__name__}: {e}")

def test_photon_reverse():
    """Test Photon reverse geocoding API"""
    print("\n" + "=" * 80)
    print("Testing Photon Reverse Geocoding API")
    print("=" * 80)
    
    url = "https://photon.komoot.io/reverse"
    lat = 21.8670864
    lon = 96.1015355
    params = {
        'lat': lat,
        'lon': lon
    }
    
    print(f"\nURL: {url}")
    print(f"Coordinates: {lat}, {lon}")
    print("\nTest 1: No headers")
    
    try:
        response = requests.get(url, params=params, timeout=10, verify=True)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Success! Features found: {len(data.get('features', []))}")
            
            if data.get('features'):
                feature = data['features'][0]
                print(f"\nFirst result:")
                print(json.dumps(feature, indent=2, ensure_ascii=False))
        else:
            print(f"✗ Failed with status {response.status_code}")
            print(f"Response: {response.text[:200]}")
    
    except Exception as e:
        print(f"✗ Error: {type(e).__name__}: {e}")

def test_with_session():
    """Test using requests.Session"""
    print("\n" + "=" * 80)
    print("Testing with requests.Session")
    print("=" * 80)
    
    session = requests.Session()
    
    url = "https://photon.komoot.io/api/"
    params = {'q': 'سقطرى'}
    
    print(f"\nURL: {url}")
    print("Using session...")
    
    try:
        response = session.get(url, params=params, timeout=10)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Success! Features found: {len(data.get('features', []))}")
        else:
            print(f"✗ Failed with status {response.status_code}")
    
    except Exception as e:
        print(f"✗ Error: {type(e).__name__}: {e}")

if __name__ == '__main__':
    test_photon_search()
    test_photon_reverse()
    test_with_session()
    
    print("\n" + "=" * 80)
    print("Tests Complete")
    print("=" * 80)
