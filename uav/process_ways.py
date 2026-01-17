import json
import requests
import time
import sys
import os
from pymongo import MongoClient
from dotenv import load_dotenv
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from check.address import validate_nominatim_result

# Load environment variables
load_dotenv()

class WayProcessor:
    def __init__(self, filename, country):
        self.filename = filename
        self.country = country
        
        # MongoDB connection
        mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        self.client = MongoClient(mongo_uri)
        self.db = self.client['osm_addresses']
        self.uav_collection = self.db['uav']
        self.validated_collection = self.db['validated_addresses']
        
        # Create session with browser-like headers for Photon
        self.photon_session = requests.Session()
        self.photon_session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        })
        
        # Statistics
        self.stats = {
            'total': 0,
            'empty_nominatim': 0,
            'found_photon': 0,
            'validated_score_1': 0,
            'saved_uav': 0,
            'skipped_mismatch': 0,
            'errors': 0
        }
    
    def query_nominatim_lookup(self, way_id):
        """Query Nominatim by OSM way ID"""
        url = "https://nominatim.openstreetmap.org/lookup"
        params = {
            'osm_ids': f'W{way_id}',
            'format': 'json',
            'addressdetails': 1
        }
        headers = {'User-Agent': 'UAV-Processor/1.0', 'accept-language': 'en'}
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            time.sleep(1)  # Rate limit
            
            if response.status_code == 200:
                return response.json(), None
            else:
                return None, f"HTTP {response.status_code}"
        except Exception as e:
            return None, str(e)
    
    def query_nominatim_reverse(self, lat, lon):
        """Query Nominatim reverse geocoding by coordinates"""
        url = "https://nominatim.openstreetmap.org/reverse"
        params = {
            'lat': lat,
            'lon': lon,
            'format': 'json',
            'addressdetails': 1
        }
        headers = {'User-Agent': 'UAV-Processor/1.0'}
        
        print(f"    Nominatim reverse: {lat}, {lon}")
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            time.sleep(1)  # Rate limit
            
            if response.status_code == 200:
                result = response.json()
                print(f"    Nominatim reverse response: {result.get('osm_type', '')}{result.get('osm_id', '')}")
                return result, None
            else:
                return None, f"HTTP {response.status_code}"
        except requests.exceptions.Timeout:
            return None, "Timeout"
        except requests.exceptions.ConnectionError:
            return None, "Connection error"
        except Exception as e:
            return None, str(e)
    
    def query_photon_search(self, name, country):
        """Query Photon API by name and country"""
        url = "https://photon.komoot.io/api/"
        query = f"{name}, {country}"
        params = {
            'q': query
        }
        
        print(f"    Photon search: {query}")
        
        try:
            response = self.photon_session.get(url, params=params, timeout=10)
            time.sleep(2)  # Rate limit - be nice to Photon
            
            if response.status_code == 200:
                result = response.json()
                print(f"    Photon response: {len(result.get('features', []))} features")
                return result, None
            else:
                return None, f"HTTP {response.status_code}"
        except requests.exceptions.Timeout:
            return None, "Timeout"
        except requests.exceptions.ConnectionError:
            return None, "Connection error"
        except Exception as e:
            return None, str(e)
    
    def query_photon_reverse(self, lat, lon):
        """Query Photon API by coordinates"""
        url = "https://photon.komoot.io/reverse"
        params = {
            'lat': lat,
            'lon': lon
        }
        
        print(f"    Photon reverse: {lat}, {lon}")
        
        try:
            response = self.photon_session.get(url, params=params, timeout=10)
            time.sleep(2)  # Rate limit - be nice to Photon
            
            if response.status_code == 200:
                result = response.json()
                print(f"    Photon response: {len(result.get('features', []))} features")
                return result, None
            else:
                return None, f"HTTP {response.status_code}"
        except requests.exceptions.Timeout:
            return None, "Timeout"
        except requests.exceptions.ConnectionError:
            return None, "Connection error"
        except Exception as e:
            return None, str(e)
    
    def build_address_from_photon(self, properties):
        """Build address string from Photon result properties"""
        parts = []
        
        fields = ['name', 'city', 'district', 'state', 'postcode', 'country']
        for field in fields:
            if field in properties and properties[field]:
                parts.append(properties[field])
        
        return ', '.join(parts)
    
    def extract_nominatim_fields(self, result):
        """Extract country, city, street from Nominatim result"""
        address = result.get('address', {})
        
        # Extract city
        city_fields = ['city', 'town', 'village', 'municipality', 'suburb', 'district']
        city = None
        for field in city_fields:
            if field in address:
                city = address[field]
                break
        
        # Extract street
        street_fields = ['road', 'street', 'pedestrian', 'path', 'footway']
        street = None
        for field in street_fields:
            if field in address:
                street = address[field]
                break
        
        # Extract country
        country = address.get('country', None)
        
        return country, city, street
    
    def process_way(self, way):
        """Process a single way"""
        self.stats['total'] += 1
        way_id = way['id']
        way_name = way['tags'].get('name', '')
        
        print(f"\n[{self.stats['total']}] Processing W{way_id}: {way_name}")
        
        # Query Nominatim
        nominatim_result, error = self.query_nominatim_lookup(way_id)
        
        if error:
            print(f"  ❌ Nominatim error: {error}")
            self.stats['errors'] += 1
            return
        
        # Case 1: Empty Nominatim result
        if not nominatim_result or len(nominatim_result) == 0:
            print(f"  ⚠️  Empty Nominatim result, trying Photon...")
            self.stats['empty_nominatim'] += 1
            self.handle_empty_nominatim(way_id, way_name)
        
        # Case 2: Nominatim result exists
        else:
            print(f"  ✓ Nominatim result found")
            self.handle_nominatim_result(way_id, way_name, nominatim_result[0])
    
    def handle_empty_nominatim(self, way_id, way_name):
        """Handle case when Nominatim returns empty result"""
        # Query Photon
        photon_result, error = self.query_photon_search(way_name, self.country)
        
        if error or not photon_result:
            print(f"  ❌ Photon error: {error}")
            return
        
        features = photon_result.get('features', [])
        if len(features) == 0:
            print(f"  ⚠️  No Photon results")
            return
        
        print(f"  🔍 Searching through {len(features)} Photon results for matching OSM ID...")
        
        # Search for matching OSM ID in all features
        matching_feature = None
        for feature in features:
            properties = feature.get('properties', {})
            photon_osm_id = properties.get('osm_id', '')
            photon_osm_type = properties.get('osm_type', '')
            
            if photon_osm_id == way_id:
                matching_feature = feature
                print(f"  ✓ Found match: {photon_osm_type}{photon_osm_id}")
                break
        
        # If no match found, skip
        if not matching_feature:
            print(f"  ⏭️  No matching OSM ID found in {len(features)} Photon results (looking for W{way_id})")
            self.stats['skipped_mismatch'] += 1
            return
        
        # Process the matching feature
        properties = matching_feature.get('properties', {})
        geometry = matching_feature.get('geometry', {})
        coordinates = geometry.get('coordinates', [])
        
        if len(coordinates) < 2:
            print(f"  ❌ Invalid Photon coordinates")
            return
        
        photon_osm_type = properties.get('osm_type', '')
        photon_osm_id = properties.get('osm_id', '')
        photon_country = properties.get('country', '')
        
        # Build address
        address = self.build_address_from_photon(properties)
        longitude, latitude = coordinates[0], coordinates[1]
        
        # Save to MongoDB
        data = {
            'address': address,
            'latitude': latitude,
            'longitude': longitude,
            'label': 'found address in photon but not in nominatim',
            'status': 1,
            'country': photon_country,
            'extra': {
                'origin_osm': f'W{way_id}',
                'photon_osm': f'{photon_osm_type}{photon_osm_id}',
                'name': way_name
            }
        }
        
        try:
            self.uav_collection.insert_one(data)
            print(f"  ✓ Saved to UAV collection: {address}")
            self.stats['found_photon'] += 1
        except Exception as e:
            print(f"  ❌ Error saving to UAV: {e}")
    
    def handle_nominatim_result(self, way_id, way_name, nominatim_result):
        """Handle case when Nominatim returns result"""
        display_name = nominatim_result.get('display_name', '')
        
        # Validate with check/address.py
        score = validate_nominatim_result(nominatim_result)
        
        # If score == 1, save to validated_addresses
        if score == 1:
            print(f"  ✓ Validation score: 1.0")
            country, city, street = self.extract_nominatim_fields(nominatim_result)
            
            data = {
                'country': country,
                'city': city,
                'street': street,
                'address': display_name,
                'status': 1,
                'score': 1,
                'osm_id': f'W{way_id}'
            }
            
            try:
                self.validated_collection.update_one(
                    {'address': data['address']},
                    {'$set': data},
                    upsert=True
                )
                print(f"  ✓ Saved to validated_addresses: {display_name}")
                self.stats['validated_score_1'] += 1
            except Exception as e:
                print(f"  ❌ Error saving to validated: {e}")
            
            return
        
        # Get coordinates from Nominatim
        lat = nominatim_result.get('lat')
        lon = nominatim_result.get('lon')
        nominatim_osm_id = nominatim_result.get('osm_id')
        
        # Extract country from Nominatim result
        nominatim_address = nominatim_result.get('address', {})
        nominatim_country = nominatim_address.get('country', '')
        
        if not lat or not lon:
            print(f"  ❌ No coordinates in Nominatim result")
            return
        
        # Query Nominatim reverse
        nominatim_reverse_result, error = self.query_nominatim_reverse(lat, lon)
        
        if error or not nominatim_reverse_result:
            print(f"  ❌ Nominatim reverse error: {error}")
            return
        
        reverse_osm_id = nominatim_reverse_result.get('osm_id')
        reverse_osm_type = nominatim_reverse_result.get('osm_type', '')
        
        # Check if OSM IDs match
        if reverse_osm_id != nominatim_osm_id:
            print(f"  ⏭️  OSM ID match: Original={nominatim_osm_id}, Reverse={reverse_osm_id}")
            self.stats['skipped_mismatch'] += 1
            return
        
        # Save to UAV collection
        data = {
            'address': display_name,
            'latitude': float(lat),
            'longitude': float(lon),
            'label': '',
            'status': 0,
            'country': nominatim_country,
            'extra': {
                'origin_osm': f'W{way_id}',
                'reverse_osm': f'{reverse_osm_type}{reverse_osm_id}',
                'name': way_name
            }
        }
        
        try:
            self.uav_collection.insert_one(data)
            print(f"  ✓ Saved to UAV collection: {display_name}")
            self.stats['saved_uav'] += 1
        except Exception as e:
            print(f"  ❌ Error saving to UAV: {e}")
    
    def run(self):
        """Main processing loop"""
        print("=" * 80)
        print("WAY PROCESSOR")
        print("=" * 80)
        print(f"File: {self.filename}")
        print(f"Country: {self.country}\n")
        
        # Load JSON file - check if it exists in current directory or uav folder
        if os.path.exists(self.filename):
            filepath = self.filename
        elif os.path.exists(os.path.join('uav', self.filename)):
            filepath = os.path.join('uav', self.filename)
        else:
            print(f"❌ Error: File not found: {self.filename}")
            print(f"   Tried: {self.filename}")
            print(f"   Tried: uav/{self.filename}")
            sys.exit(1)
        
        print(f"Loading: {filepath}\n")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        ways = data.get('ways', [])
        print(f"Found {len(ways)} ways to process\n")
        
        # Process each way
        for way in ways:
            self.process_way(way)
        
        # Print statistics
        self.print_stats()
    
    def print_stats(self):
        """Print processing statistics"""
        print("\n" + "=" * 80)
        print("PROCESSING COMPLETE")
        print("=" * 80)
        print(f"Total processed: {self.stats['total']}")
        print(f"Empty Nominatim: {self.stats['empty_nominatim']}")
        print(f"Found in Photon: {self.stats['found_photon']}")
        print(f"Validated (score=1): {self.stats['validated_score_1']}")
        print(f"Saved to UAV: {self.stats['saved_uav']}")
        print(f"Skipped (mismatch): {self.stats['skipped_mismatch']}")
        print(f"Errors: {self.stats['errors']}")

def main():
    if len(sys.argv) < 3:
        print("Usage: python process_ways.py <filename> <country>")
        print("Example: python process_ways.py all_ways_ye.json Yemen")
        sys.exit(1)
    
    filename = sys.argv[1]
    country = sys.argv[2]
    
    processor = WayProcessor(filename, country)
    processor.run()

if __name__ == '__main__':
    main()
