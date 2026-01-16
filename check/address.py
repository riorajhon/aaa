"""
Validate Nominatim result through multiple checks.
Returns False or score (0-1.0).

Input: Nominatim API result (single result object)
Output: False (if validation fails) or score (float)
"""

import requests
import math
import re


def compute_bounding_box_area_meters(boundingbox):
    """
    Compute bounding box area in square meters.
    
    Args:
        boundingbox: List [south, north, west, east] as strings
        
    Returns:
        Area in square meters (float)
    """
    south, north, west, east = map(float, boundingbox)
    
    # Approximate center latitude for longitude scaling
    center_lat = (south + north) / 2.0
    lat_m = 111_000  # meters per degree latitude
    lon_m = 111_000 * math.cos(math.radians(center_lat))  # meters per degree longitude
    
    height_m = abs(north - south) * lat_m
    width_m = abs(east - west) * lon_m
    area_m2 = width_m * height_m
    
    return area_m2


def looks_like_address(address):
    """
    Check if string looks like a valid address.
    
    Args:
        address: Address string to validate
        
    Returns:
        True if looks like address, False otherwise
    """
    address = address.strip().lower()
    
    # Length checks
    address_len = re.sub(r'[^\w]', '', address.strip(), flags=re.UNICODE)
    if len(address_len) < 30:
        return False
    if len(address_len) > 300:
        return False
    
    # Letter count
    letter_count = len(re.findall(r'[^\W\d]', address, flags=re.UNICODE))
    if letter_count < 20:
        return False
    
    # Must have letters
    if re.match(r"^[^a-zA-Z]*$", address):
        return False
    
    # Character diversity
    if len(set(address)) < 5:
        return False
    
    # Must have numbers in at least one section
    address_for_number_count = address.replace('-', '').replace(';', '')
    sections = [s.strip() for s in address_for_number_count.split(',')]
    sections_with_numbers = []
    for section in sections:
        number_groups = re.findall(r"[0-9]+", section)
        if len(number_groups) > 0:
            sections_with_numbers.append(section)
    
    if len(sections_with_numbers) < 1:
        return False
    
    # Must have at least 2 commas
    if address.count(",") < 2:
        return False
    
    # Check for invalid special characters
    special_chars = ['`', ':', '%', '@', '*', '^', '[', ']', '{', '}', '_', '«', '»']
    if any(char in address for char in special_chars):
        return False
    
    return True


def validate_address_region(address, country):
    """
    Validate that address contains the expected country.
    
    Args:
        address: Full address string
        country: Expected country name
        
    Returns:
        True if country matches, False otherwise
    """
    if not address or not country:
        return False
    
    address_lower = address.lower()
    country_lower = country.lower()
    
    # Simple check: country appears in address
    return country_lower in address_lower


def check_with_nominatim(address):
    """
    Query Nominatim and calculate score based on bounding box.
    
    Args:
        address: Address string to validate
        
    Returns:
        Score (0.0-1.0) or 0.0 if validation fails
    """
    try:
        url = "https://nominatim.openstreetmap.org/search"
        params = {"q": address, "format": "json"}
        headers = {"User-Agent": "UAV-Validator/1.0"}
        
        response = requests.get(url, params=params, headers=headers, timeout=5)
        results = response.json()
        
        if len(results) == 0:
            return 0.0
        
        # Extract numbers from original address
        original_numbers = set(re.findall(r"[0-9]+", address.lower()))
        
        # Filter results
        filtered_results = []
        for result in results:
            # Check place_rank >= 20
            place_rank = result.get('place_rank', 0)
            if place_rank < 20:
                continue
            
            # Check name field exists and is in address
            name = result.get('name', '')
            if name and name.lower() not in address.lower():
                continue
            
            # Check numbers match
            display_name = result.get('display_name', '')
            if display_name:
                display_numbers = set(re.findall(r"[0-9]+", display_name.lower()))
                if original_numbers and display_numbers != original_numbers:
                    continue
            
            filtered_results.append(result)
        
        if len(filtered_results) == 0:
            return 0.0
        
        # Calculate total area
        total_area = 0
        for result in results:
            if 'boundingbox' in result:
                area = compute_bounding_box_area_meters(result['boundingbox'])
                total_area += area
        
        # Score based on total area
        if total_area < 100:
            score = 1.0
        elif total_area < 1000:
            score = 0.9
        elif total_area < 10000:
            score = 0.8
        elif total_area < 100000:
            score = 0.7
        else:
            score = 0.3
        
        return score
        
    except Exception as e:
        return 0.0


def validate_nominatim_result(nominatim_result):
    """
    Main validation function.
    
    Args:
        nominatim_result: Single Nominatim API result object (dict)
        
    Returns:
        False if any validation fails, otherwise returns score (float 0.0-1.0)
    """
    # Extract required fields
    boundingbox = nominatim_result.get('boundingbox')
    display_name = nominatim_result.get('display_name', '')
    address_dict = nominatim_result.get('address', {})
    
    # Extract country from result
    country = address_dict.get('country', '')
    
    if not boundingbox or not display_name or not country:
        return False
    
    # Check 1: Bounding box area
    area = compute_bounding_box_area_meters(boundingbox)
    if area > 100:
        return False
    
    # Check 2: Looks like address
    if not looks_like_address(display_name):
        return False
    
    # Check 3: Region validation
    if not validate_address_region(display_name, country):
        return False
    
    # Check 4: Get score from Nominatim
    score = check_with_nominatim(display_name)
    
    return score


# Example usage
if __name__ == "__main__":
    # Example Nominatim result
    nominatim_result = {
        "place_id": 196055816,
        "licence": "Data © OpenStreetMap contributors, ODbL 1.0. http://osm.org/copyright",
        "osm_type": "node",
        "osm_id": 4591597199,
        "lat": "36.7096289",
        "lon": "67.1163936",
        "class": "shop",
        "type": "computer",
        "place_rank": 30,
        "importance": 5.8413824673792895e-05,
        "addresstype": "shop",
        "name": "فروشگاه رایانه مظفر",
        "display_name": "فروشگاه رایانه مظفر, 1701, Massoud Shahid Boulevard, Mazar-i-Sharif, Balkh Province, 1701, Afghanistan",
        "address": {
            "shop": "فروشگاه رایانه مظفر",
            "house_number": "1701",
            "road": "Massoud Shahid Boulevard",
            "city": "Mazar-i-Sharif",
            "state": "Balkh Province",
            "ISO3166-2-lvl4": "AF-BAL",
            "postcode": "1701",
            "country": "Afghanistan",
            "country_code": "af"
        },
        "extratags": {
            "operator": "Computer Technology Market"
        },
        "boundingbox": [
            "36.7095789",
            "36.7096789",
            "67.1163436",
            "67.1164436"
        ]
    }
    
    # country = 'Afghanistan'
    
    result = validate_nominatim_result(nominatim_result)
    
    if result is False:
        print("Validation failed")
    else:
        print(f"Validation passed with score: {result}")
