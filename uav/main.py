#!/usr/bin/env python3
"""
Main orchestration script for UAV address processing pipeline.

Pipeline:
1. Loop through countries in country_names.json
2. Find country code from geonames_countries.json
3. Check for OSM data in osm_data/ folder
4. Download if missing using URLs from urls.py
5. Export ways using export_all_ways.py
6. Process ways using process_ways.py
"""

import json
import os
import sys
import subprocess
import requests
from pathlib import Path

class UAVPipeline:
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.osm_data_dir = self.base_dir / 'osm_data'
        
        # Ensure osm_data directory exists
        self.osm_data_dir.mkdir(exist_ok=True)
        
        # Load required data
        self.countries = self.load_countries()
        self.geonames_countries = self.load_geonames_countries()
        self.urls = self.load_urls()
        
        # Statistics
        self.stats = {
            'total_countries': 0,
            'found_codes': 0,
            'downloaded': 0,
            'exported': 0,
            'processed': 0,
            'skipped': 0,
            'errors': 0
        }
    
    def load_countries(self):
        """Load country names from country_names.json"""
        try:
            with open(self.base_dir / 'country_names.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"[ERROR] Error loading country_names.json: {e}")
            sys.exit(1)
    
    def load_geonames_countries(self):
        """Load geonames countries mapping"""
        try:
            with open(self.base_dir / 'geonames_countries.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"[ERROR] Error loading geonames_countries.json: {e}")
            sys.exit(1)
    
    def load_urls(self):
        """Load URLs from urls.py"""
        try:
            sys.path.insert(0, str(self.base_dir))
            from urls import GEOFABRIK_URLS
            return GEOFABRIK_URLS
        except Exception as e:
            print(f"[ERROR] Error loading urls.py: {e}")
            sys.exit(1)
    
    def find_country_code(self, country_name):
        """Find country code from geonames data"""
        country_name_lower = country_name.lower()
        
        for code, data in self.geonames_countries.items():
            if data['name'].lower() == country_name_lower:
                return code.lower()
        
        return None
    
    def check_osm_file_exists(self, country_code):
        """Check if OSM PBF file exists"""
        osm_file = self.osm_data_dir / f'{country_code}-latest.osm.pbf'
        return osm_file.exists()
    
    def download_osm_file(self, country_code):
        """Download OSM PBF file using URLs from urls.py"""
        if country_code.upper() not in self.urls:
            print(f"  [ERROR] No download URL found for {country_code.upper()}")
            return False
        
        url = self.urls[country_code.upper()]
        osm_file = self.osm_data_dir / f'{country_code}-latest.osm.pbf'
        
        print(f"  [DOWNLOAD] Downloading {country_code}-latest.osm.pbf...")
        print(f"     URL: {url}")
        
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            last_percent = -1
            
            with open(osm_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        if total_size > 0:
                            percent = int((downloaded / total_size) * 100)
                            # Only print every 10% to reduce output noise
                            if percent != last_percent and percent % 10 == 0:
                                print(f"     Progress: {percent}%")
                                last_percent = percent
            
            print(f"  [OK] Downloaded successfully: {osm_file.name}")
            return True
            
        except Exception as e:
            print(f"  [ERROR] Download failed: {e}")
            if osm_file.exists():
                osm_file.unlink()  # Remove partial file
            return False
    
    def export_ways(self, country_code):
        """Export ways using export_all_ways.py"""
        print(f"  [EXPORT] Exporting ways for {country_code}...")
        
        try:
            # Run export_all_ways.py with country code
            result = subprocess.run([
                sys.executable, 
                str(self.base_dir / 'export_all_ways.py'),
                country_code
            ], capture_output=True, text=True, cwd=self.base_dir)
            
            if result.returncode == 0:
                print(f"  [OK] Ways exported successfully")
                return True
            else:
                print(f"  [ERROR] Export failed:")
                print(f"     {result.stderr}")
                return False
                
        except Exception as e:
            print(f"  [ERROR] Export error: {e}")
            return False
    
    def process_ways(self, country_code, country_name):
        """Process ways using process_ways.py"""
        json_file = f'all_ways_{country_code}.json'
        print(f"  [PROCESS] Processing ways: {json_file} with country '{country_name}'...")
        
        try:
            # Run process_ways.py with json file and country name
            result = subprocess.run([
                sys.executable,
                str(self.base_dir / 'process_ways.py'),
                json_file,
                country_name
            ], capture_output=True, text=True, cwd=self.base_dir)
            
            if result.returncode == 0:
                print(f"  [OK] Ways processed successfully")
                return True
            else:
                print(f"  [ERROR] Processing failed:")
                print(f"     {result.stderr}")
                return False
                
        except Exception as e:
            print(f"  [ERROR] Processing error: {e}")
            return False
    
    def process_country(self, country_name):
        """Process a single country through the entire pipeline"""
        print(f"\\n{'='*60}")
        print(f"PROCESSING: {country_name}")
        print(f"{'='*60}")
        
        # Step 1: Find country code
        country_code = self.find_country_code(country_name)
        if not country_code:
            print(f"  [ERROR] Country code not found for '{country_name}'")
            self.stats['skipped'] += 1
            return False
        
        print(f"  [OK] Country code: {country_code.upper()}")
        self.stats['found_codes'] += 1
        
        # Step 2: Check/Download OSM data
        if self.check_osm_file_exists(country_code):
            print(f"  [OK] OSM file exists: {country_code}-latest.osm.pbf")
        else:
            print(f"  [WARNING] OSM file missing, downloading...")
            if not self.download_osm_file(country_code):
                self.stats['errors'] += 1
                return False
            self.stats['downloaded'] += 1
        
        # Step 3: Export ways
        if not self.export_ways(country_code):
            self.stats['errors'] += 1
            return False
        self.stats['exported'] += 1
        
        # Step 4: Process ways
        if not self.process_ways(country_code, country_name):
            self.stats['errors'] += 1
            return False
        self.stats['processed'] += 1
        
        print(f"  [SUCCESS] {country_name} completed successfully!")
        return True
    
    def run(self):
        """Main pipeline execution"""
        print("=" * 80)
        print("UAV ADDRESS PROCESSING PIPELINE")
        print("=" * 80)
        print(f"Countries to process: {len(self.countries)}")
        print(f"OSM data directory: {self.osm_data_dir}")
        print("=" * 80)
        
        self.stats['total_countries'] = len(self.countries)
        
        # Process each country
        for i, country_name in enumerate(self.countries, 1):
            print(f"\\n[{i}/{len(self.countries)}] Starting {country_name}...")
            
            try:
                self.process_country(country_name)
            except KeyboardInterrupt:
                print(f"\\n\\n[WARNING] Pipeline interrupted by user!")
                break
            except Exception as e:
                print(f"\\n[ERROR] Unexpected error processing {country_name}: {e}")
                self.stats['errors'] += 1
                continue
        
        # Print final statistics
        self.print_final_stats()
    
    def print_final_stats(self):
        """Print final pipeline statistics"""
        print("\\n" + "=" * 80)
        print("PIPELINE COMPLETE")
        print("=" * 80)
        print(f"Total countries: {self.stats['total_countries']}")
        print(f"Found country codes: {self.stats['found_codes']}")
        print(f"Downloaded OSM files: {self.stats['downloaded']}")
        print(f"Exported ways: {self.stats['exported']}")
        print(f"Processed ways: {self.stats['processed']}")
        print(f"Skipped: {self.stats['skipped']}")
        print(f"Errors: {self.stats['errors']}")
        print("=" * 80)
        
        # Save JSON report
        self.save_report()
    
    def save_report(self):
        """Save simple JSON report with counts"""
        report = {
            "total_countries": self.stats['total_countries'],
            "found_codes": self.stats['found_codes'],
            "downloaded": self.stats['downloaded'],
            "exported": self.stats['exported'],
            "processed": self.stats['processed'],
            "skipped": self.stats['skipped'],
            "errors": self.stats['errors']
        }
        
        report_file = self.base_dir / 'pipeline_report.json'
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2)
            print(f"[REPORT] Report saved: {report_file}")
        except Exception as e:
            print(f"[ERROR] Error saving report: {e}")

def main():
    pipeline = UAVPipeline()
    
    try:
        pipeline.run()
    except KeyboardInterrupt:
        print("\\n[WARNING] Pipeline interrupted!")
        sys.exit(1)
    except Exception as e:
        print(f"\\n[ERROR] Pipeline failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()