import json
from datetime import datetime

def count_records_in_date_range():
    """Count email records between 2025-04-01 and 2025-05-26"""
    
    json_file = r"D:\office\migration\clariverseai\backend\data-type\EU-bank\formatting\email\email-data.json"
    start_date = datetime(2025, 4, 1)
    end_date = datetime(2025, 5, 26)
    
    print("Loading email data...")
    
    try:
        with open(json_file, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        total_records = len(data)
        count_in_range = 0
        
        print(f"Total records: {total_records:,}")
        print(f"Date range: 2025-04-01 to 2025-05-26")
        print("Analyzing...")
        
        for record in data:
            try:
                # Get first_message_at from thread
                first_message_str = record['thread']['first_message_at']
                # Parse ISO format: 2025-01-15T09:32:00Z
                # Remove the 'Z' and parse as naive datetime
                first_message_date = datetime.fromisoformat(first_message_str.replace('Z', ''))
                
                # Extract just the date part for comparison (ignore time)
                first_message_date_only = first_message_date.date()
                start_date_only = start_date.date()
                end_date_only = end_date.date()
                
                # Check if in range
                if start_date_only <= first_message_date_only <= end_date_only:
                    count_in_range += 1
                    
            except (KeyError, ValueError) as e:
                # Skip records with missing or invalid dates
                continue
        
        percentage = (count_in_range / total_records) * 100 if total_records > 0 else 0
        
        print("\n" + "="*50)
        print("RESULTS")
        print("="*50)
        print(f"Records in date range: {count_in_range:,}")
        print(f"Percentage of total: {percentage:.2f}%")
        print("="*50)
        
        return count_in_range
        
    except FileNotFoundError:
        print(f"Error: File not found at {json_file}")
        return 0
    except json.JSONDecodeError:
        print("Error: Invalid JSON format")
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 0

# Run the analysis
if __name__ == "__main__":
    count_records_in_date_range()
