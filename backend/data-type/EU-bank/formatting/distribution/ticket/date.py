# Import required libraries
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import random

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

# Connect to MongoDB
client = MongoClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DATABASE_NAME]

# Get collection
tickets_collection = db['tickets_new']

def generate_ticket_start_times(num_records, start_date, end_date):
    """
    Generate realistic start times for tickets distributed across 6 months
    Ensures good distribution with at least one ticket per day
    """
    total_days = (end_date - start_date).days + 1
    
    start_times = []
    
    print(f"  Distributing {num_records} tickets across {total_days} days...")
    print(f"  Target average: {num_records / total_days:.1f} tickets per day")
    
    # STEP 1: Ensure every day gets at least ONE ticket (if we have enough records)
    if num_records >= total_days:
        print(f"  Ensuring coverage for all {total_days} days...")
        for day_offset in range(total_days):
            day_date = start_date + timedelta(days=day_offset)
            
            # Ticket creation time (business hours weighted)
            # Peak hours: 9 AM - 5 PM (business hours)
            hour = random.choices(
                range(0, 24),
                weights=[1,1,1,1,1,2,3,5,8,12,15,18,18,16,15,14,12,8,5,3,2,1,1,1],
                k=1
            )[0]
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            
            start_time = day_date.replace(hour=hour, minute=minute, second=second)
            start_times.append(start_time)
        
        # STEP 2: Distribute remaining tickets randomly
        remaining_tickets = num_records - total_days
        print(f"  Distributing {remaining_tickets} additional tickets...")
        
        for _ in range(remaining_tickets):
            random_day = random.randint(0, total_days - 1)
            day_date = start_date + timedelta(days=random_day)
            
            hour = random.choices(
                range(0, 24),
                weights=[1,1,1,1,1,2,3,5,8,12,15,18,18,16,15,14,12,8,5,3,2,1,1,1],
                k=1
            )[0]
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            
            start_time = day_date.replace(hour=hour, minute=minute, second=second)
            start_times.append(start_time)
    else:
        # If fewer tickets than days, just distribute them
        print(f"  Distributing {num_records} tickets randomly...")
        for _ in range(num_records):
            random_day = random.randint(0, total_days - 1)
            day_date = start_date + timedelta(days=random_day)
            
            hour = random.choices(
                range(0, 24),
                weights=[1,1,1,1,1,2,3,5,8,12,15,18,18,16,15,14,12,8,5,3,2,1,1,1],
                k=1
            )[0]
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            
            start_time = day_date.replace(hour=hour, minute=minute, second=second)
            start_times.append(start_time)
    
    # Sort by date so tickets are in chronological order
    start_times.sort()
    
    return start_times

def generate_ticket_message_times(start_time, message_count):
    """
    Generate realistic timestamps for ticket messages
    Tickets typically have quick responses within same day or next business day
    
    Args:
        start_time: datetime when ticket is created
        message_count: number of messages in the ticket (typically 4-7)
    
    Returns:
        List of datetime objects for each message
    """
    message_times = [start_time]
    current_time = start_time
    
    # For tickets, most conversations are same-day or span 1-2 days
    # 70% same day, 25% span 2 days, 5% span 3 days
    span_probability = random.random()
    
    if span_probability < 0.70:
        # Same day conversation (70% of tickets)
        max_span = 1
    elif span_probability < 0.95:
        # Spans 2 days (25% of tickets)
        max_span = 2
    else:
        # Spans 3 days (5% of tickets)
        max_span = 3
    
    messages_per_day = message_count // max_span
    remainder = message_count % max_span
    
    day_message_counts = [messages_per_day] * max_span
    for i in range(remainder):
        day_message_counts[i] += 1
    
    current_day = 0
    messages_in_current_day = 1  # Already added first message
    
    for i in range(1, message_count):
        # Check if we should move to next day
        if current_day < max_span - 1 and messages_in_current_day >= day_message_counts[current_day]:
            # Move to next business day
            current_day += 1
            messages_in_current_day = 0
            
            # Jump to next day at realistic business hours (8 AM - 11 AM)
            next_day = current_time.date() + timedelta(days=1)
            next_hour = random.randint(8, 11)
            next_minute = random.randint(0, 59)
            current_time = datetime.combine(next_day, datetime.min.time())
            current_time = current_time.replace(hour=next_hour, minute=next_minute, second=random.randint(0, 59))
        else:
            # Response times within same day
            # Tickets have faster response times than regular emails
            response_type = random.choices(
                ['quick', 'normal', 'delayed', 'long'],
                weights=[40, 35, 20, 5],  # Most responses are quick or normal
                k=1
            )[0]
            
            if response_type == 'quick':
                # 5-30 minutes (urgent tickets)
                minutes_to_add = random.randint(5, 30)
                current_time += timedelta(minutes=minutes_to_add)
            elif response_type == 'normal':
                # 30 minutes - 2 hours (standard response)
                minutes_to_add = random.randint(30, 120)
                current_time += timedelta(minutes=minutes_to_add)
            elif response_type == 'delayed':
                # 2-4 hours (busy periods)
                hours_to_add = random.randint(2, 4)
                current_time += timedelta(hours=hours_to_add)
            else:  # long
                # 4-6 hours (requires investigation)
                hours_to_add = random.randint(4, 6)
                current_time += timedelta(hours=hours_to_add)
        
        message_times.append(current_time)
        messages_in_current_day += 1
    
    return message_times

def fix_ticket_dates(record, ticket_start_time):
    """
    Fix dates in a single ticket record
    Updates only: thread.first_message_at, thread.last_message_at, messages.[].headers.date
    
    Args:
        record: Single ticket record
        ticket_start_time: datetime when this ticket should start
    """
    # Determine message count
    if 'messages' in record:
        message_count = len(record['messages'])
    elif 'thread' in record and 'message_count' in record['thread']:
        message_count = record['thread']['message_count']
    else:
        # Default for tickets (4-7 range)
        message_count = random.randint(4, 7)
    
    # Generate realistic ticket message times
    message_times = generate_ticket_message_times(ticket_start_time, message_count)
    
    # Update thread-level dates (first and last message times)
    if 'thread' not in record:
        record['thread'] = {}
    
    record['thread']['first_message_at'] = message_times[0].strftime("%Y-%m-%dT%H:%M:%S")
    record['thread']['last_message_at'] = message_times[-1].strftime("%Y-%m-%dT%H:%M:%S")
    
    # Update individual message timestamps in headers.date
    if 'messages' in record:
        for i, message in enumerate(record['messages']):
            if i < len(message_times):
                timestamp_str = message_times[i].strftime("%Y-%m-%dT%H:%M:%S")
                
                # Ensure headers exists
                if 'headers' not in message:
                    message['headers'] = {}
                
                # Update only headers.date
                message['headers']['date'] = timestamp_str
    
    return record

def process_ticket_records_from_db():
    """
    Process all ticket records from MongoDB and update them with realistic dates
    """
    # Date range
    start_date = datetime(2025, 1, 1, 0, 0, 0)
    end_date = datetime(2025, 6, 30, 23, 59, 59)
    
    print(f"Reading ticket records from MongoDB collection: tickets_new")
    
    # Get all records from the collection
    records = list(tickets_collection.find())
    print(f"✓ Loaded {len(records)} ticket records from database")
    
    if not records:
        print("No records found in the collection. Exiting.")
        return
    
    # Debug: Show structure of first record
    if records:
        print(f"\nDebug: First record structure:")
        first_record = records[0]
        print(f"  Top-level keys: {list(first_record.keys())}")
        if 'thread' in first_record:
            print(f"  Thread keys: {list(first_record['thread'].keys())}")
        if 'messages' in first_record and first_record['messages']:
            print(f"  First message keys: {list(first_record['messages'][0].keys())}")
            if 'headers' in first_record['messages'][0]:
                print(f"  First message headers keys: {list(first_record['messages'][0]['headers'].keys())}")
        print()
    
    print(f"\nGenerating realistic ticket start times...")
    print("✓ Tickets distributed across all days")
    print("✓ Most tickets resolved same day (70%)")
    print("✓ Some span 2-3 days for complex issues (30%)")
    print("✓ Response times: 5 min - 6 hours depending on urgency")
    
    # Generate start times for all tickets
    start_times = generate_ticket_start_times(len(records), start_date, end_date)
    
    # Process each record
    print(f"\nProcessing {len(records)} tickets...")
    
    stats = {
        'total_messages': 0,
        'resolution_times_minutes': [],
        'tickets_per_day': {},
        'same_day_tickets': 0,
        'multi_day_tickets': 0,
        'ticket_days_span': [],
        'updated_count': 0
    }
    
    for i, record in enumerate(records, 1):
        try:
            ticket_start = start_times[i-1]
            
            # Track tickets per day
            day_key = ticket_start.strftime("%Y-%m-%d")
            stats['tickets_per_day'][day_key] = stats['tickets_per_day'].get(day_key, 0) + 1
            
            fixed_record = fix_ticket_dates(record, ticket_start)
            
            # Debug: Show what was updated for first few records
            if i <= 3:
                print(f"\nDebug: Ticket {i} updates:")
                if 'thread' in fixed_record:
                    print(f"  Thread first_message_at: {fixed_record['thread'].get('first_message_at')}")
                    print(f"  Thread last_message_at: {fixed_record['thread'].get('last_message_at')}")
                if 'messages' in fixed_record and fixed_record['messages']:
                    print(f"  First message headers.date: {fixed_record['messages'][0].get('headers', {}).get('date')}")
                    print(f"  Last message headers.date: {fixed_record['messages'][-1].get('headers', {}).get('date')}")
            
            # Calculate resolution time and check if multi-day
            if 'messages' in fixed_record and len(fixed_record['messages']) > 0:
                first_msg = fixed_record['messages'][0]
                last_msg = fixed_record['messages'][-1]
                
                first_time = datetime.strptime(first_msg['headers']['date'], "%Y-%m-%dT%H:%M:%S")
                last_time = datetime.strptime(last_msg['headers']['date'], "%Y-%m-%dT%H:%M:%S")
                
                resolution_minutes = (last_time - first_time).total_seconds() / 60
                stats['resolution_times_minutes'].append(resolution_minutes)
                
                # Check if multi-day
                days_span = (last_time.date() - first_time.date()).days + 1
                stats['ticket_days_span'].append(days_span)
                
                if days_span == 1:
                    stats['same_day_tickets'] += 1
                else:
                    stats['multi_day_tickets'] += 1
                
                stats['total_messages'] += len(fixed_record['messages'])
            
            # Update the record in the database
            tickets_collection.update_one(
                {'_id': record['_id']},
                {'$set': fixed_record}
            )
            stats['updated_count'] += 1
            
            # Progress
            if i % 50 == 0:
                print(f"  Processed {i}/{len(records)} tickets...")
                
        except Exception as e:
            print(f"⚠ Error processing record {i}: {e}")
            import traceback
            traceback.print_exc()
    
    # Calculate statistics
    days_with_tickets = len(stats['tickets_per_day'])
    total_days = 181
    days_without_tickets = total_days - days_with_tickets
    
    tickets_per_day_values = list(stats['tickets_per_day'].values())
    avg_tickets_per_day = sum(tickets_per_day_values) / len(tickets_per_day_values) if tickets_per_day_values else 0
    max_tickets_per_day = max(tickets_per_day_values) if tickets_per_day_values else 0
    min_tickets_per_day = min(tickets_per_day_values) if tickets_per_day_values else 0
    
    # Print statistics
    print(f"\n{'='*70}")
    print("TICKET STATISTICS")
    print(f"{'='*70}")
    print(f"✓ Total tickets:              {len(records)}")
    print(f"✓ Date range:                 2025-01-01 to 2025-06-30 ({total_days} days)")
    print(f"✓ Days with tickets:          {days_with_tickets}")
    print(f"✓ Days without tickets:       {days_without_tickets}")
    print(f"✓ Avg tickets per day:        {avg_tickets_per_day:.1f}")
    print(f"✓ Max tickets in one day:     {max_tickets_per_day}")
    print(f"✓ Min tickets in one day:     {min_tickets_per_day}")
    print(f"✓ Total messages:             {stats['total_messages']}")
    print(f"✓ Avg messages per ticket:    {stats['total_messages'] / len(records):.1f}")
    print(f"✓ Records updated:            {stats['updated_count']}")
    
    print(f"\n--- RESOLUTION STATISTICS ---")
    print(f"✓ Same-day resolutions:       {stats['same_day_tickets']} ({stats['same_day_tickets']/len(records)*100:.1f}%)")
    print(f"✓ Multi-day tickets:          {stats['multi_day_tickets']} ({stats['multi_day_tickets']/len(records)*100:.1f}%)")
    
    if stats['ticket_days_span']:
        max_days = max(stats['ticket_days_span'])
        print(f"✓ Longest ticket span:        {max_days} days")
    
    if stats['resolution_times_minutes']:
        avg_resolution = sum(stats['resolution_times_minutes']) / len(stats['resolution_times_minutes'])
        max_resolution = max(stats['resolution_times_minutes'])
        min_resolution = min(stats['resolution_times_minutes'])
        print(f"\n--- TIME TO RESOLUTION ---")
        print(f"✓ Avg resolution time:        {avg_resolution:.1f} minutes ({avg_resolution/60:.1f} hours)")
        print(f"✓ Longest resolution:         {max_resolution:.1f} minutes ({max_resolution/60:.1f} hours)")
        print(f"✓ Quickest resolution:        {min_resolution:.1f} minutes")
    
    # Sample output - show examples
    if len(records) >= 5:
        print(f"\n{'='*70}")
        print("SAMPLE TICKETS")
        print(f"{'='*70}")
        
        # Show first 3 examples
        for idx in range(min(3, len(records))):
            rec = records[idx]
            if 'messages' in rec and len(rec['messages']) > 0:
                first_msg = rec['messages'][0]
                last_msg = rec['messages'][-1]
                
                first_time = datetime.strptime(first_msg['headers']['date'], "%Y-%m-%dT%H:%M:%S")
                last_time = datetime.strptime(last_msg['headers']['date'], "%Y-%m-%dT%H:%M:%S")
                
                resolution_minutes = (last_time - first_time).total_seconds() / 60
                days_span = (last_time.date() - first_time.date()).days + 1
                
                print(f"\n📋 Ticket {idx + 1}:")
                print(f"  Created:    {first_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"  Resolved:   {last_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"  Duration:   {resolution_minutes:.1f} minutes ({resolution_minutes/60:.1f} hours)")
                print(f"  Messages:   {len(rec['messages'])}")
                print(f"  Span:       {days_span} day{'s' if days_span > 1 else ''}")
                if days_span > 1:
                    print(f"  Type:       Multi-day ticket (complex issue)")
                else:
                    print(f"  Type:       Same-day resolution")

# Run the script
if __name__ == "__main__":
    print("Starting ticket date processing from MongoDB...")
    process_ticket_records_from_db()
    print("\nTicket date processing completed!")
    
    # Close the database connection
    client.close()