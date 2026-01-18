import os
import sys
from pymongo import MongoClient
from dotenv import load_dotenv
from collections import defaultdict

# Load environment variables
load_dotenv()

class UAVCleaner:
    def __init__(self):
        # MongoDB connection
        mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        self.client = MongoClient(mongo_uri)
        self.db = self.client['osm_addresses']
        self.uav_collection = self.db['uav']
        
        # Statistics
        self.stats = {
            'total_uavs': 0,
            'status_0_uavs': 0,
            'node_reverse_deleted': 0,
            'duplicate_groups': 0,
            'duplicates_deleted': 0,
            'remaining_uavs': 0
        }
    
    def clean_node_reverse_osm(self):
        """Delete UAVs where extra.reverse_osm starts with 'N' (nodes)"""
        print("🔍 Finding UAVs with node reverse_osm...")
        
        # Find UAVs with status=0 and reverse_osm starting with 'N'
        query = {
            'status': 0,
            'extra.reverse_osm': {'$regex': '^N'}
        }
        
        # Count first
        total_count = self.uav_collection.count_documents(query)
        print(f"Found {total_count:,} UAVs with node reverse_osm")
        
        if total_count == 0:
            return
        
        # Delete in batches using limit
        batch_size = 1000
        deleted_total = 0
        
        while True:
            # Find batch of documents to delete
            docs_to_delete = list(self.uav_collection.find(query, {'_id': 1}).limit(batch_size))
            
            if not docs_to_delete:
                break
            
            # Extract IDs
            ids_to_delete = [doc['_id'] for doc in docs_to_delete]
            
            # Delete batch
            result = self.uav_collection.delete_many({'_id': {'$in': ids_to_delete}})
            deleted_count = result.deleted_count
            deleted_total += deleted_count
            
            print(f"  Deleted batch: {deleted_count:,} (Total: {deleted_total:,}/{total_count:,})")
            
            if deleted_count == 0:
                break
        
        self.stats['node_reverse_deleted'] = deleted_total
        print(f"✓ Deleted {deleted_total:,} UAVs with node reverse_osm\n")
    
    def remove_duplicate_addresses(self):
        """Remove duplicate UAVs based on address field"""
        print("🔍 Finding duplicate addresses...")
        
        # Aggregation pipeline to find duplicates with limit
        pipeline = [
            # Only status=0 UAVs
            {'$match': {'status': 0}},
            
            # Group by address and collect IDs
            {'$group': {
                '_id': '$address',
                'ids': {'$push': '$_id'},
                'count': {'$sum': 1}
            }},
            
            # Only groups with more than 1 document (duplicates)
            {'$match': {'count': {'$gt': 1}}},
            
            # Sort by count descending to see biggest duplicate groups first
            {'$sort': {'count': -1}},
            
            # Limit to process in batches
            {'$limit': 1000}
        ]
        
        batch_size = 500
        total_deleted = 0
        total_groups_processed = 0
        
        while True:
            # Get batch of duplicate groups
            duplicate_groups = list(self.uav_collection.aggregate(pipeline))
            
            if not duplicate_groups:
                break
            
            print(f"Processing {len(duplicate_groups):,} duplicate groups...")
            
            # Show top 3 duplicate groups in this batch
            if total_groups_processed == 0:
                print("Sample duplicate groups:")
                for i, group in enumerate(duplicate_groups[:3]):
                    address = group['_id'][:80] + "..." if len(group['_id']) > 80 else group['_id']
                    print(f"  {i+1}. '{address}' - {group['count']} duplicates")
                print()
            
            # Process duplicates in this batch
            batch_deleted = 0
            for group in duplicate_groups:
                ids_to_delete = group['ids'][1:]  # Keep first, delete rest
                
                # Delete in smaller batches
                for i in range(0, len(ids_to_delete), batch_size):
                    batch_ids = ids_to_delete[i:i + batch_size]
                    
                    result = self.uav_collection.delete_many({
                        '_id': {'$in': batch_ids}
                    })
                    
                    deleted_count = result.deleted_count
                    batch_deleted += deleted_count
                    total_deleted += deleted_count
            
            total_groups_processed += len(duplicate_groups)
            print(f"  Deleted {batch_deleted:,} duplicates from {len(duplicate_groups):,} groups")
            print(f"  Total processed: {total_groups_processed:,} groups, {total_deleted:,} deleted")
            
            # If we got less than the limit, we're done
            if len(duplicate_groups) < 1000:
                break
        
        self.stats['duplicate_groups'] = total_groups_processed
        self.stats['duplicates_deleted'] = total_deleted
        print(f"✓ Deleted {total_deleted:,} duplicate UAVs from {total_groups_processed:,} groups\n")
    
    def get_statistics(self):
        """Get current UAV statistics"""
        # Total UAVs
        self.stats['total_uavs'] = self.uav_collection.count_documents({})
        
        # Status=0 UAVs
        self.stats['status_0_uavs'] = self.uav_collection.count_documents({'status': 0})
        
        # Remaining after cleanup
        self.stats['remaining_uavs'] = self.stats['total_uavs']
    
    def print_statistics(self):
        """Print cleanup statistics"""
        print("=" * 60)
        print("CLEANUP STATISTICS")
        print("=" * 60)
        print(f"Total UAVs: {self.stats['total_uavs']:,}")
        print(f"Status=0 UAVs: {self.stats['status_0_uavs']:,}")
        print(f"Node reverse_osm deleted: {self.stats['node_reverse_deleted']:,}")
        print(f"Duplicate groups found: {self.stats['duplicate_groups']:,}")
        print(f"Duplicate UAVs deleted: {self.stats['duplicates_deleted']:,}")
        print(f"Remaining UAVs: {self.stats['remaining_uavs']:,}")
        print("=" * 60)
    
    def run(self):
        """Main cleanup process"""
        print("=" * 60)
        print("UAV CLEANUP TOOL")
        print("=" * 60)
        print("This tool will:")
        print("1. Delete UAVs with node reverse_osm (status=0)")
        print("2. Remove duplicate addresses (status=0)")
        print("=" * 60)
        print()
        
        # Get initial statistics
        self.get_statistics()
        print(f"Initial UAVs: {self.stats['total_uavs']:,}")
        print(f"Status=0 UAVs: {self.stats['status_0_uavs']:,}")
        print()
        
        # Step 1: Clean node reverse_osm
        self.clean_node_reverse_osm()
        
        # Step 2: Remove duplicates
        self.remove_duplicate_addresses()
        
        # Final statistics
        self.get_statistics()
        self.print_statistics()

def main():
    cleaner = UAVCleaner()
    
    try:
        cleaner.run()
    except KeyboardInterrupt:
        print("\n⚠️  Cleanup interrupted by user!")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error during cleanup: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()