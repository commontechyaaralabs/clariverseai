import json
from datetime import datetime, timedelta
import random

def parse_date(date_string):
    """Parse ISO date string to datetime object"""
    try:
        return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ")
    except:
        try:
            return datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        except:
            return None

def calculate_original_follow_up_difference(record):
    """
    Calculate the difference in days between last_message_at and follow_up_date
    in the original record
    """
    if 'follow_up_date' not in record or not record['follow_up_date']:
        return None
    
    last_message_date = parse_date(record['thread']['last_message_at'])
    follow_up_date = parse_date(record['follow_up_date'])
    
    if last_message_date and follow_up_date:
        difference = (follow_up_date - last_message_date).days
        return difference
    
    return None

def generate_realistic_start_times(num_records, start_date, end_date):
    """
    Generate realistic start times for emails distributed across 6 months
    GUARANTEES at least one email per day, then distributes remaining emails
    """
    total_days = (end_date - start_date).days + 1  # Include last day
    
    start_times = []
    
    # STEP 1: Ensure every day gets at least ONE email
    print(f"  Ensuring coverage for all {total_days} days...")
    for day_offset in range(total_days):
        day_date = start_date + timedelta(days=day_offset)
        
        # Random time of day (business hours weighted)
        hour = random.choices(
            range(0, 24),
            weights=[1,1,1,1,1,1,2,3,5,8,10,10,10,10,8,8,6,4,2,1,1,1,1,1],
            k=1
        )[0]
        minute = random.randint(0, 59)
        
        start_time = day_date.replace(hour=hour, minute=minute, second=0)
        start_times.append(start_time)
    
    # STEP 2: Distribute remaining emails randomly across days
    remaining_emails = num_records - total_days
    print(f"  Distributing {remaining_emails} additional emails...")
    
    for _ in range(remaining_emails):
        # Random day in range
        random_day = random.randint(0, total_days - 1)
        day_date = start_date + timedelta(days=random_day)
        
        # Random time of day (business hours weighted)
        hour = random.choices(
            range(0, 24),
            weights=[1,1,1,1,1,1,2,3,5,8,10,10,10,10,8,8,6,4,2,1,1,1,1,1],
            k=1
        )[0]
        minute = random.randint(0, 59)
        
        start_time = day_date.replace(hour=hour, minute=minute, second=0)
        start_times.append(start_time)
    
    # Sort by date so emails are in chronological order
    start_times.sort()
    
    return start_times

def fix_dates_in_record(record, first_message_date, start_date, end_date):
    """
    Fix dates in a single email record preserving the original follow-up difference
    
    Args:
        record: Single email thread record
        first_message_date: The new date for first message
        start_date: datetime object for start date (for bounds checking)
        end_date: datetime object for end date (for bounds checking)
    """
    # Calculate original follow-up difference BEFORE changing dates
    original_follow_up_diff = calculate_original_follow_up_difference(record)
    
    # Get message count
    message_count = record['thread']['message_count']
    
    # Parse original dates to understand the pattern
    original_dates = []
    for msg in record['messages']:
        date = parse_date(msg['headers']['date'])
        if date:
            original_dates.append(date)
    
    # Calculate original spacing between messages
    original_spacings = []
    if len(original_dates) > 1:
        for i in range(1, len(original_dates)):
            days_diff = (original_dates[i] - original_dates[i-1]).days
            hours_diff = (original_dates[i] - original_dates[i-1]).seconds // 3600
            original_spacings.append((days_diff, hours_diff))
    
    # Generate new message dates
    message_dates = []
    current_date = first_message_date
    message_dates.append(current_date)
    
    # Use original spacing pattern if available, otherwise use realistic defaults
    for i in range(1, message_count):
        if i-1 < len(original_spacings):
            # Use original spacing pattern
            days_to_add, hours_to_add = original_spacings[i-1]
            # Make spacing more realistic (between 1-14 days typically)
            days_to_add = max(1, min(days_to_add, 14))
        else:
            # Default realistic spacing
            days_to_add = random.randint(1, 7)
            hours_to_add = random.randint(0, 12)
        
        current_date = current_date + timedelta(days=days_to_add, hours=hours_to_add)
        
        # Ensure we don't exceed end_date
        if current_date > end_date:
            current_date = end_date - timedelta(hours=random.randint(1, 24))
        
        message_dates.append(current_date)
    
    # Update thread-level dates
    record['thread']['first_message_at'] = message_dates[0].strftime("%Y-%m-%dT%H:%M:%SZ")
    record['thread']['last_message_at'] = message_dates[-1].strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Update individual message dates
    for i, message in enumerate(record['messages']):
        if i < len(message_dates):
            message['headers']['date'] = message_dates[i].strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Update follow_up_date using ORIGINAL difference
    if 'follow_up_date' in record and original_follow_up_diff is not None:
        # Calculate new follow_up_date by adding original difference to new last_message_at
        new_follow_up = message_dates[-1] + timedelta(days=original_follow_up_diff)
        record['follow_up_date'] = new_follow_up.strftime("%Y-%m-%dT%H:%M:%SZ")
    elif 'follow_up_date' in record and record['follow_up_date']:
        # If we couldn't calculate original difference, use default (7-21 days)
        follow_up_days = random.randint(7, 21)
        new_follow_up = message_dates[-1] + timedelta(days=follow_up_days)
        record['follow_up_date'] = new_follow_up.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    return record

def process_all_records(input_file, output_file):
    """
    Process all records in a JSON/JSONL file with realistic date distribution
    
    Args:
        input_file: Path to input file
        output_file: Path to output file
    """
    # Date range
    start_date = datetime(2025, 1, 1, 0, 0, 0)
    end_date = datetime(2025, 6, 30, 23, 59, 59)
    
    print(f"Reading from: {input_file}")
    
    # Read the file
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read().strip()
    
    # Try to parse as JSON array first
    records = []
    try:
        data = json.loads(content)
        if isinstance(data, list):
            records = data
        else:
            records = [data]
        print(f"✓ Loaded {len(records)} records as JSON array")
    except json.JSONDecodeError:
        # Try JSONL format (one JSON object per line)
        print("Not a JSON array, trying JSONL format...")
        for line_num, line in enumerate(content.split('\n'), 1):
            if line.strip():
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"⚠ Error on line {line_num}: {e}")
        print(f"✓ Loaded {len(records)} records as JSONL")
    
    print(f"\nGenerating realistic date distribution for {len(records)} records...")
    print("✓ GUARANTEED: At least 1 email per day")
    print("✓ Remaining emails distributed randomly (multiple per day possible)")
    
    # Generate realistic start times for all records
    start_times = generate_realistic_start_times(len(records), start_date, end_date)
    
    # Fix dates in all records
    print(f"\nProcessing {len(records)} records...")
    fixed_records = []
    
    stats = {
        'same_day_count': 0,
        'follow_up_preserved': 0,
        'follow_up_calculated': 0
    }
    
    for i, record in enumerate(records, 1):
        try:
            # Get the pre-assigned start time for this record
            first_message_date = start_times[i-1]
            
            # Check if same day as previous
            if i > 1 and start_times[i-1].date() == start_times[i-2].date():
                stats['same_day_count'] += 1
            
            # Check if follow-up will be preserved
            if calculate_original_follow_up_difference(record) is not None:
                stats['follow_up_preserved'] += 1
            
            fixed_record = fix_dates_in_record(record, first_message_date, start_date, end_date)
            fixed_records.append(fixed_record)
            
            # Progress indicator
            if i % 200 == 0:
                print(f"  Processed {i}/{len(records)} records...")
        except Exception as e:
            print(f"⚠ Error processing record {i}: {e}")
            fixed_records.append(record)  # Keep original if error
    
    # Write output as JSON array
    print(f"\nWriting to: {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(fixed_records, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ Done! Fixed {len(fixed_records)} records")
    print(f"✓ Date range: 2025-01-01 to 2025-06-30")
    print(f"✓ Records sharing same day: {stats['same_day_count']}")
    print(f"✓ Follow-up differences preserved: {stats['follow_up_preserved']}")
    
    # Show sample of first few records
    if len(fixed_records) >= 3:
        print("\n--- Sample (First 3 Records) ---")
        for idx in range(3):
            rec = fixed_records[idx]
            print(f"\nRecord {idx + 1}:")
            print(f"  First message: {rec['thread']['first_message_at']}")
            print(f"  Last message:  {rec['thread']['last_message_at']}")
            print(f"  Message count: {rec['thread']['message_count']}")
            if 'follow_up_date' in rec and rec['follow_up_date']:
                last = parse_date(rec['thread']['last_message_at'])
                follow = parse_date(rec['follow_up_date'])
                if last and follow:
                    diff = (follow - last).days
                    print(f"  Follow-up date: {rec['follow_up_date']} (+{diff} days)")

# Run the script
if __name__ == "__main__":
    # CHANGE THESE PATHS TO YOUR FILES
    input_file = "full_email_data.json"      # Your original file with 2004 records
    output_file = "output_emails2.json"    # Fixed output file
    
    process_all_records(input_file, output_file)