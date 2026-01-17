import osmium
import sys
import json

class WayExporter(osmium.SimpleHandler):
    def __init__(self):
        osmium.SimpleHandler.__init__(self)
        self.way_count = 0
        self.total_processed = 0
        self.ways_data = []  # Store ways as JSON objects
        
    def way(self, w):
        self.total_processed += 1
        
        # Get tags
        tags = {tag.k: tag.v for tag in w.tags}
        
        # Filter: skip if no name:* tags (like name:en, name:fr, etc.)
        has_name_variant = any(k.startswith('name:') for k in tags.keys())
        if not has_name_variant:
            return
        
        self.way_count += 1
        
        # Build way data as JSON object (only id and tags)
        way_data = {
            'id': w.id,
            'tags': tags
        }
        
        # Add to collection
        self.ways_data.append(way_data)
        
        # Progress indicator
        if self.total_processed % 1000 == 0:
            print(f"Processed {self.total_processed:,} ways... (with name:* tags: {self.way_count:,})")

def main():
    # Command line arguments
    if len(sys.argv) > 1:
        country_code = sys.argv[1]
        input_filename = f'uav/osm_data/{country_code}-latest.osm.pbf'
        output_filename = f'uav/all_ways_{country_code}.json'
    else:
        print("Usage: python export_all_ways.py <country_code>")
        print("Example: python export_all_ways.py ye")
        sys.exit(1)
    
    print(f"Exporting ways with name:* tags from {input_filename} to {output_filename}...")
    print("This may take a moment...\n")
    
    handler = WayExporter()
    
    try:
        handler.apply_file(input_filename)
        
        # Prepare output data
        output_data = {
            'source': input_filename,
            'total_ways_processed': handler.total_processed,
            'ways_with_name_tags': handler.way_count,
            'ways': handler.ways_data
        }
        
        # Write to JSON file
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n✓ Export complete!")
        print(f"Total ways processed: {handler.total_processed:,}")
        print(f"Ways with name:* tags exported: {handler.way_count:,}")
        print(f"Output file: {output_filename}")
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user!")
        
        # Save partial results
        output_data = {
            'source': input_filename,
            'total_ways_processed': handler.total_processed,
            'ways_with_name_tags': handler.way_count,
            'interrupted': True,
            'ways': handler.ways_data
        }
        
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"Partial results saved to: {output_filename}")
        sys.exit(1)

if __name__ == '__main__':
    main()
