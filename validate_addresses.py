import requests
import time
import json
import sys
import os
from pymongo import MongoClient
from dotenv import load_dotenv
from check.address_check import looks_like_address, validate_address_region, compute_bounding_box_areas_meters
from check.address_score import check_with_nominatim

# Load environment variables
load_dotenv()

class AddressValidator:
    def __init__(self):
        # MongoDB connection from .env
        mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        self.client = MongoClient(mongo_uri)
        self.db = self.client['osm_addresses']
        self.collection = self.db['validated_addresses']
        
        # Output files
        self.uav_candidates_file = open('uav_candidates.txt', 'w', encoding='utf-8')
        self.errors = []
        
        # Statistics
        self.stats = {
            'total': 0,
            'skipped_bbox': 0,
            'skipped_looks': 0,
            'skipped_region': 0,
            'skipped_score': 0,
            'saved_to_db': 0,
            'empty_results': 0,
            'reverse_match': 0,
            'errors': 0
        }
    
    def query_nominatim_lookup(self, node_id):
        """Query Nominatim by OSM ID"""
        url = "https://nominatim.openstreetmap.org/lookup"
        params = {
            'osm_ids': f'N{node_id}',
            'format': 'json',
            'addressdetails': 1
        }
        headers = {'User-Agent': 'UAV-Miner/1.0'}
        
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
        """Query Nominatim by coordinates"""
        url = "https://nominatim.openstreetmap.org/reverse"
        params = {
            'lat': lat,
            'lon': lon,
            'format': 'json',
            'addressdetails': 1
        }
        headers = {'User-Agent': 'UAV-Miner/1.0'}
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            time.sleep(1)  # Rate limit
            
            if response.status_code == 200:
                return response.json(), None
            else:
                return None, f"HTTP {response.status_code}"
        except Exception as e:
            return None, str(e)
    
    def extract_address_fields(self, address_dict):
        """Extract city, country, street from address"""
        # Extract city
        city_fields = ['city', 'town', 'village', 'municipality', 'suburb', 'district']
        city = None
        for field in city_fields:
            if field in address_dict:
                city = address_dict[field]
                break
        
        # Extract street
        street_fields = ['road', 'street', 'pedestrian', 'path', 'footway']
        street = None
        for field in street_fields:
            if field in address_dict:
                street = address_dict[field]
                break
        
        # Extract country
        country = address_dict.get('country', None)
        
        return city, country, street
    
    def process_node(self, node_id, lat, lon):
        """Process a single node"""
        self.stats['total'] += 1
        
        print(f"\n[{self.stats['total']}] Processing N{node_id}...")
        
        # Query Nominatim by OSM ID
        result, error = self.query_nominatim_lookup(node_id)
        
        if error:
            print(f"  ❌ Error: {error}")
            self.stats['errors'] += 1
            self.errors.append({
                'node_id': node_id,
                'lat': lat,
                'lon': lon,
                'error': error
            })
            return
        
        # Case 1: Result exists
        if result and len(result) > 0:
            self.process_existing_result(node_id, lat, lon, result[0])
        
        # Case 2: Empty result
        else:
            print(f"  ⚠️  Empty result, trying reverse geocoding...")
            self.stats['empty_results'] += 1
            self.process_empty_result(node_id, lat, lon)
    
    def process_existing_result(self, node_id, lat, lon, result):
        """Process when Nominatim returns a result"""
        display_name = result.get('display_name', '')
        address_dict = result.get('address', {})
        boundingbox = result.get('boundingbox', [])
        
        # Check bounding box area
        if boundingbox:
            area = compute_bounding_box_areas_meters(boundingbox)
            print(f"  Bbox area: {area:.2f} m²")
            
            if area > 100:
                print(f"  ⏭️  Skipped: bbox too large ({area:.2f} > 100)")
                self.stats['skipped_bbox'] += 1
                return
        
        # Check looks_like_address
        if not looks_like_address(display_name):
            print(f"  ⏭️  Skipped: doesn't look like address")
            self.stats['skipped_looks'] += 1
            return
        
        # Extract country for validation
        _, country, _ = self.extract_address_fields(address_dict)
        
        if not country:
            print(f"  ⏭️  Skipped: no country found")
            self.stats['skipped_region'] += 1
            return
        
        # Check validate_address_region
        if not validate_address_region(display_name, country):
            print(f"  ⏭️  Skipped: region validation failed")
            self.stats['skipped_region'] += 1
            return
        
        # Check score
        score = check_with_nominatim(display_name)
        print(f"  Score: {score}")
        
        if score < 1:
            print(f"  ⏭️  Skipped: score too low ({score} < 1)")
            self.stats['skipped_score'] += 1
            return
        
        # Extract all fields
        city, country, street = self.extract_address_fields(address_dict)
        
        # Save to MongoDB
        address_data = {
            'osm_id': f'N{node_id}',
            'country': country,
            'city': city,
            'street': street,
            'score': score,
            'status': 1,
            'address': display_name
        }
        
        try:
            self.collection.update_one(
                {'address': address_data['address']},
                {'$set': address_data},
                upsert=True
            )
            print(f"  ✓ Saved: {display_name} : {score}")
            self.stats['saved_to_db'] += 1
        except Exception as e:
            print(f"  ❌ Error saving to DB: {e}")
            self.stats['errors'] += 1
    
    def process_empty_result(self, node_id, lat, lon):
        """Process when Nominatim returns empty result"""
        # Query by coordinates
        result, error = self.query_nominatim_reverse(lat, lon)
        
        if error:
            print(f"  ❌ Reverse geocoding error: {error}")
            self.stats['errors'] += 1
            return
        
        if not result:
            print(f"  ⚠️  No reverse geocoding result")
            return
        
        # Check if OSM ID matches
        result_osm_id = result.get('osm_id', None)
        result_osm_type = result.get('osm_type', '')
        
        if result_osm_type == 'node' and result_osm_id == int(node_id):
            print(f"  ✓ Reverse match! Saving to txt...")
            
            display_name = result.get('display_name', '')
            address_dict = result.get('address', {})
            country = address_dict.get('country', '')
            
            # Save to txt file: fulladdress,country,node_id,lat,lon
            self.uav_candidates_file.write(f"{display_name},{country},N{node_id},{lat},{lon}\n")
            self.uav_candidates_file.flush()
            
            self.stats['reverse_match'] += 1
        else:
            print(f"  ⏭️  OSM ID mismatch: expected N{node_id}, got {result_osm_type}{result_osm_id}")
    
    def run(self, input_file):
        """Main processing loop"""
        print("=" * 80)
        print("ADDRESS VALIDATION - STAGE 2")
        print("=" * 80)
        print(f"\nReading candidates from: {input_file}")
        print(f"MongoDB: {self.collection.count_documents({})} existing addresses\n")
        
        # Read candidate nodes
        with open(input_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()[1:]  # Skip header
        
        print(f"Found {len(lines)} candidates to process\n")
        
        # Process each node
        for line in lines:
            parts = line.strip().split()
            if len(parts) != 3:
                continue
            
            node_id, lat, lon = parts[0], parts[1], parts[2]
            self.process_node(node_id, lat, lon)
        
        # Cleanup and save errors
        self.uav_candidates_file.close()
        
        if self.errors:
            with open('processing_errors.json', 'w', encoding='utf-8') as f:
                json.dump(self.errors, f, indent=2, ensure_ascii=False)
        
        # Print statistics
        self.print_stats()
    
    def print_stats(self):
        """Print processing statistics"""
        print("\n" + "=" * 80)
        print("PROCESSING COMPLETE")
        print("=" * 80)
        print(f"Total processed: {self.stats['total']}")
        print(f"Saved to DB: {self.stats['saved_to_db']}")
        print(f"UAV candidates (reverse match): {self.stats['reverse_match']}")
        print(f"\nSkipped:")
        print(f"  - Bbox too large: {self.stats['skipped_bbox']}")
        print(f"  - Doesn't look like address: {self.stats['skipped_looks']}")
        print(f"  - Region validation failed: {self.stats['skipped_region']}")
        print(f"  - Score too low: {self.stats['skipped_score']}")
        print(f"  - Empty results: {self.stats['empty_results']}")
        print(f"\nErrors: {self.stats['errors']}")
        print(f"\nOutput files:")
        print(f"  - uav_candidates.txt")
        print(f"  - processing_errors.json")

def main():
    input_file = 'candidate_node.txt'
    
    validator = AddressValidator()
    validator.run(input_file)

if __name__ == '__main__':
    main()
