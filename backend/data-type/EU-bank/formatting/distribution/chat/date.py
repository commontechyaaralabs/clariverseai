# Import required libraries
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import random
import json

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

# Connect to MongoDB
client = MongoClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DATABASE_NAME]

# Get collections
chat_new_collection = db['chat_new']

def generate_chat_start_times(num_records, start_date, end_date):
    """
    Generate realistic start times for chat conversations
    Ensures good distribution across 6 months with multiple chats per day
    
    For 600 records over 181 days = ~3.3 chats per day average
    """
    total_days = (end_date - start_date).days + 1
    
    start_times = []
    
    print(f"  Distributing {num_records} chat conversations across {total_days} days...")
    print(f"  Target average: {num_records / total_days:.1f} chats per day")
    
    # Distribute chats across all days with realistic variation
    for _ in range(num_records):
        # Random day in range
        random_day = random.randint(0, total_days - 1)
        day_date = start_date + timedelta(days=random_day)
        
        # Chat start time distribution (more during active hours)
        # Peak hours: 8 AM - 10 PM
        hour = random.choices(
            range(0, 24),
            weights=[1,1,1,1,1,2,3,5,8,12,15,16,15,15,14,15,16,15,13,11,9,6,3,2],
            k=1
        )[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        start_time = day_date.replace(hour=hour, minute=minute, second=second)
        start_times.append(start_time)
    
    # Sort by date so chats are in chronological order
    start_times.sort()
    
    return start_times

def should_conversation_span_multiple_days(message_count):
    """
    Decide if conversation should span multiple days based on message count
    and random probability
    
    Returns: (spans_multiple_days, num_days)
    """
    # More messages = higher chance of multi-day conversation
    if message_count <= 10:
        # Short conversations - 90% same day, 10% spans 2 days
        if random.random() < 0.10:
            return True, 2
        return False, 1
    
    elif message_count <= 15:
        # Medium conversations - 70% same day, 20% two days, 10% three days
        rand = random.random()
        if rand < 0.20:
            return True, 2
        elif rand < 0.30:
            return True, 3
        return False, 1
    
    else:
        # Longer conversations (16-20+ messages) - more likely to span days
        # 50% same day, 30% two days, 15% three days, 5% four+ days
        rand = random.random()
        if rand < 0.30:
            return True, 2
        elif rand < 0.45:
            return True, 3
        elif rand < 0.50:
            return True, random.randint(4, 5)
        return False, 1

def generate_chat_message_times(start_time, message_count):
    """
    Generate realistic timestamps for chat messages
    Can span multiple days with overnight gaps
    
    Args:
        start_time: datetime when chat starts
        message_count: number of messages in the conversation
    
    Returns:
        List of datetime objects for each message
    """
    message_times = [start_time]
    current_time = start_time
    
    # Decide if conversation spans multiple days
    spans_days, num_days = should_conversation_span_multiple_days(message_count)
    
    if spans_days:
        # Split messages across days
        messages_per_day = message_count // num_days
        remainder = message_count % num_days
        
        day_message_counts = [messages_per_day] * num_days
        # Distribute remainder
        for i in range(remainder):
            day_message_counts[i] += 1
        
        current_day = 0
        messages_in_current_day = 1  # Already added first message
        
        for i in range(1, message_count):
            # Check if we should move to next day
            if current_day < num_days - 1 and messages_in_current_day >= day_message_counts[current_day]:
                # Move to next day (overnight gap)
                current_day += 1
                messages_in_current_day = 0
                
                # Jump to next day at a realistic time (8 AM - 11 AM typically)
                next_day = current_time.date() + timedelta(days=1)
                next_hour = random.randint(8, 11)
                next_minute = random.randint(0, 59)
                current_time = datetime.combine(next_day, datetime.min.time())
                current_time = current_time.replace(hour=next_hour, minute=next_minute, second=random.randint(0, 59))
            else:
                # Normal spacing within same day
                gap_type = random.choices(
                    ['instant', 'quick', 'normal', 'thinking', 'delay'],
                    weights=[15, 35, 30, 12, 8],
                    k=1
                )[0]
                
                if gap_type == 'instant':
                    seconds_to_add = random.randint(2, 15)
                    current_time += timedelta(seconds=seconds_to_add)
                elif gap_type == 'quick':
                    seconds_to_add = random.randint(15, 60)
                    current_time += timedelta(seconds=seconds_to_add)
                elif gap_type == 'normal':
                    minutes_to_add = random.randint(1, 4)
                    current_time += timedelta(minutes=minutes_to_add)
                elif gap_type == 'thinking':
                    minutes_to_add = random.randint(4, 10)
                    current_time += timedelta(minutes=minutes_to_add)
                else:  # delay
                    minutes_to_add = random.randint(10, 30)
                    current_time += timedelta(minutes=minutes_to_add)
            
            message_times.append(current_time)
            messages_in_current_day += 1
    
    else:
        # Single day conversation - normal chat spacing
        for i in range(1, message_count):
            gap_type = random.choices(
                ['instant', 'quick', 'normal', 'thinking', 'delay', 'long_pause'],
                weights=[15, 35, 30, 12, 6, 2],
                k=1
            )[0]
            
            if gap_type == 'instant':
                seconds_to_add = random.randint(2, 15)
                current_time += timedelta(seconds=seconds_to_add)
            elif gap_type == 'quick':
                seconds_to_add = random.randint(15, 60)
                current_time += timedelta(seconds=seconds_to_add)
            elif gap_type == 'normal':
                minutes_to_add = random.randint(1, 4)
                current_time += timedelta(minutes=minutes_to_add)
            elif gap_type == 'thinking':
                minutes_to_add = random.randint(4, 10)
                current_time += timedelta(minutes=minutes_to_add)
            elif gap_type == 'delay':
                minutes_to_add = random.randint(10, 30)
                current_time += timedelta(minutes=minutes_to_add)
            else:  # long_pause
                minutes_to_add = random.randint(30, 90)
                current_time += timedelta(minutes=minutes_to_add)
            
            message_times.append(current_time)
    
    return message_times

def fix_chat_dates(record, chat_start_time):
    """
    Fix dates in a single chat conversation record
    
    Args:
        record: Single chat conversation record
        chat_start_time: datetime when this chat should start
    """
    # Determine message count
    if 'messages' in record:
        message_count = len(record['messages'])
    elif 'message_count' in record:
        message_count = record['message_count']
    elif 'chat' in record and 'message_count' in record['chat']:
        message_count = record['chat']['message_count']
    else:
        message_count = 18  # Default for chat (18-20 range)
    
    # Generate realistic chat message times (may span multiple days)
    message_times = generate_chat_message_times(chat_start_time, message_count)
    
    # Update chat-level dates if they exist
    if 'chat' in record:
        if 'createdDateTime' in record['chat']:
            record['chat']['createdDateTime'] = message_times[0].strftime("%Y-%m-%dT%H:%M:%SZ")
        if 'lastUpdatedDateTime' in record['chat']:
            record['chat']['lastUpdatedDateTime'] = message_times[-1].strftime("%Y-%m-%dT%H:%M:%SZ")
        if 'message_count' in record['chat']:
            record['chat']['message_count'] = message_count
    
    # Update conversation-level dates
    if 'createdDateTime' in record:
        record['createdDateTime'] = message_times[0].strftime("%Y-%m-%dT%H:%M:%SZ")
    if 'lastUpdatedDateTime' in record:
        record['lastUpdatedDateTime'] = message_times[-1].strftime("%Y-%m-%dT%H:%M:%SZ")
    if 'created_at' in record:
        record['created_at'] = message_times[0].strftime("%Y-%m-%dT%H:%M:%SZ")
    if 'updated_at' in record:
        record['updated_at'] = message_times[-1].strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Update individual message timestamps
    if 'messages' in record:
        for i, message in enumerate(record['messages']):
            if i < len(message_times):
                timestamp_str = message_times[i].strftime("%Y-%m-%dT%H:%M:%SZ")
                
                # Update createdDateTime field for each message
                message['createdDateTime'] = timestamp_str
                
                # Also update other possible timestamp fields if they exist
                if 'timestamp' in message:
                    message['timestamp'] = timestamp_str
                if 'created_at' in message:
                    message['created_at'] = timestamp_str
                if 'date' in message:
                    message['date'] = timestamp_str
                if 'sent_at' in message:
                    message['sent_at'] = timestamp_str
                if 'headers' in message and isinstance(message['headers'], dict):
                    if 'date' in message['headers']:
                        message['headers']['date'] = timestamp_str
    
    # Remove follow-up fields (not needed for chats)
    fields_to_remove = ['follow_up_date', 'follow_up_required', 'follow_up_reason']
    for field in fields_to_remove:
        if field in record:
            del record[field]
    
    return record

def process_chat_records_from_db():
    """
    Process all chat conversation records from MongoDB and update them with realistic dates
    """
    # Date range
    start_date = datetime(2025, 1, 1, 0, 0, 0)
    end_date = datetime(2025, 6, 30, 23, 59, 59)
    
    print(f"Reading chat records from MongoDB collection: chat_new")
    
    # Get all records from the collection
    records = list(chat_new_collection.find())
    print(f"✓ Loaded {len(records)} chat conversation records from database")
    
    if not records:
        print("No records found in the collection. Exiting.")
        return
    
    # Debug: Show structure of first record
    if records:
        print(f"\nDebug: First record structure:")
        first_record = records[0]
        print(f"  Top-level keys: {list(first_record.keys())}")
        if 'chat' in first_record:
            print(f"  Chat keys: {list(first_record['chat'].keys())}")
        if 'messages' in first_record and first_record['messages']:
            print(f"  First message keys: {list(first_record['messages'][0].keys())}")
        print()
    
    print(f"\nGenerating realistic chat start times...")
    print("✓ Different customers chatting throughout each day")
    print("✓ Some conversations span multiple days (customer comes back later)")
    print("✓ Messages spaced realistically (seconds to minutes apart)")
    
    # Generate start times for all conversations
    start_times = generate_chat_start_times(len(records), start_date, end_date)
    
    # Process each record
    print(f"\nProcessing {len(records)} chat conversations...")
    
    stats = {
        'total_messages': 0,
        'durations_minutes': [],
        'chats_per_day': {},
        'same_hour_count': 0,
        'multi_day_conversations': 0,
        'conversation_days': [],
        'updated_count': 0
    }
    
    for i, record in enumerate(records, 1):
        try:
            chat_start = start_times[i-1]
            
            # Track chats per day
            day_key = chat_start.strftime("%Y-%m-%d")
            stats['chats_per_day'][day_key] = stats['chats_per_day'].get(day_key, 0) + 1
            
            # Check if multiple chats in same hour
            if i > 1:
                prev_hour = start_times[i-2].replace(minute=0, second=0, microsecond=0)
                curr_hour = chat_start.replace(minute=0, second=0, microsecond=0)
                if prev_hour == curr_hour:
                    stats['same_hour_count'] += 1
            
            fixed_record = fix_chat_dates(record, chat_start)
            
            # Debug: Show what was updated for first few records
            if i <= 3:
                print(f"\nDebug: Record {i} updates:")
                if 'chat' in fixed_record:
                    print(f"  Chat createdDateTime: {fixed_record['chat'].get('createdDateTime')}")
                    print(f"  Chat lastUpdatedDateTime: {fixed_record['chat'].get('lastUpdatedDateTime')}")
                if 'messages' in fixed_record and fixed_record['messages']:
                    print(f"  First message createdDateTime: {fixed_record['messages'][0].get('createdDateTime')}")
                    print(f"  Last message createdDateTime: {fixed_record['messages'][-1].get('createdDateTime')}")
            
            # Calculate duration and check if multi-day
            if 'messages' in fixed_record and len(fixed_record['messages']) > 0:
                first_msg = fixed_record['messages'][0]
                last_msg = fixed_record['messages'][-1]
                
                # Get timestamps - prioritize createdDateTime
                first_time = None
                last_time = None
                
                # Check for createdDateTime first
                if 'createdDateTime' in first_msg:
                    first_time = datetime.strptime(first_msg['createdDateTime'], "%Y-%m-%dT%H:%M:%SZ")
                elif 'timestamp' in first_msg:
                    first_time = datetime.strptime(first_msg['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
                elif 'created_at' in first_msg:
                    first_time = datetime.strptime(first_msg['created_at'], "%Y-%m-%dT%H:%M:%SZ")
                elif 'date' in first_msg:
                    first_time = datetime.strptime(first_msg['date'], "%Y-%m-%dT%H:%M:%SZ")
                elif 'sent_at' in first_msg:
                    first_time = datetime.strptime(first_msg['sent_at'], "%Y-%m-%dT%H:%M:%SZ")
                elif 'headers' in first_msg and isinstance(first_msg['headers'], dict) and 'date' in first_msg['headers']:
                    first_time = datetime.strptime(first_msg['headers']['date'], "%Y-%m-%dT%H:%M:%SZ")
                
                if 'createdDateTime' in last_msg:
                    last_time = datetime.strptime(last_msg['createdDateTime'], "%Y-%m-%dT%H:%M:%SZ")
                elif 'timestamp' in last_msg:
                    last_time = datetime.strptime(last_msg['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
                elif 'created_at' in last_msg:
                    last_time = datetime.strptime(last_msg['created_at'], "%Y-%m-%dT%H:%M:%SZ")
                elif 'date' in last_msg:
                    last_time = datetime.strptime(last_msg['date'], "%Y-%m-%dT%H:%M:%SZ")
                elif 'sent_at' in last_msg:
                    last_time = datetime.strptime(last_msg['sent_at'], "%Y-%m-%dT%H:%M:%SZ")
                elif 'headers' in last_msg and isinstance(last_msg['headers'], dict) and 'date' in last_msg['headers']:
                    last_time = datetime.strptime(last_msg['headers']['date'], "%Y-%m-%dT%H:%M:%SZ")
                
                if first_time and last_time:
                    duration_minutes = (last_time - first_time).total_seconds() / 60
                    stats['durations_minutes'].append(duration_minutes)
                    
                    # Check if multi-day
                    days_span = (last_time.date() - first_time.date()).days + 1
                    stats['conversation_days'].append(days_span)
                    if days_span > 1:
                        stats['multi_day_conversations'] += 1
                
                stats['total_messages'] += len(fixed_record['messages'])
            
            # Update the record in the database
            chat_new_collection.update_one(
                {'_id': record['_id']},
                {'$set': fixed_record}
            )
            stats['updated_count'] += 1
            
            # Progress
            if i % 50 == 0:
                print(f"  Processed {i}/{len(records)} conversations...")
                
        except Exception as e:
            print(f"⚠ Error processing record {i}: {e}")
            import traceback
            traceback.print_exc()
    
    # Calculate statistics
    days_with_chats = len(stats['chats_per_day'])
    total_days = 181
    days_without_chats = total_days - days_with_chats
    
    chats_per_day_values = list(stats['chats_per_day'].values())
    avg_chats_per_day = sum(chats_per_day_values) / len(chats_per_day_values) if chats_per_day_values else 0
    max_chats_per_day = max(chats_per_day_values) if chats_per_day_values else 0
    min_chats_per_day = min(chats_per_day_values) if chats_per_day_values else 0
    
    # Print statistics
    print(f"\n{'='*70}")
    print("CHAT CONVERSATION STATISTICS")
    print(f"{'='*70}")
    print(f"✓ Total conversations:        {len(records)}")
    print(f"✓ Date range:                 2025-01-01 to 2025-06-30 ({total_days} days)")
    print(f"✓ Days with chats:            {days_with_chats}")
    print(f"✓ Days without chats:         {days_without_chats}")
    print(f"✓ Avg chats per day:          {avg_chats_per_day:.1f}")
    print(f"✓ Max chats in one day:       {max_chats_per_day}")
    print(f"✓ Min chats in one day:       {min_chats_per_day}")
    print(f"✓ Total messages:             {stats['total_messages']}")
    print(f"✓ Avg messages per chat:      {stats['total_messages'] / len(records):.1f}")
    print(f"✓ Records updated:            {stats['updated_count']}")
    print(f"\n--- MULTI-DAY CONVERSATIONS ---")
    print(f"✓ Multi-day conversations:    {stats['multi_day_conversations']} ({stats['multi_day_conversations']/len(records)*100:.1f}%)")
    print(f"✓ Single-day conversations:   {len(records) - stats['multi_day_conversations']} ({(len(records) - stats['multi_day_conversations'])/len(records)*100:.1f}%)")
    
    if stats['conversation_days']:
        max_days = max(stats['conversation_days'])
        print(f"✓ Longest conversation span:  {max_days} days")
    
    if stats['durations_minutes']:
        avg_duration = sum(stats['durations_minutes']) / len(stats['durations_minutes'])
        max_duration = max(stats['durations_minutes'])
        min_duration = min(stats['durations_minutes'])
        print(f"\n--- DURATION STATISTICS ---")
        print(f"✓ Avg total duration:         {avg_duration:.1f} minutes")
        print(f"✓ Longest conversation:       {max_duration:.1f} minutes ({max_duration/60:.1f} hours)")
        print(f"✓ Shortest conversation:      {min_duration:.1f} minutes")
    
    print(f"\n✓ Chats in same hour:         {stats['same_hour_count']} (different customers)")
    
    # Sample output - show both single-day and multi-day examples
    if len(records) >= 5:
        print(f"\n{'='*70}")
        print("SAMPLE CONVERSATIONS")
        print(f"{'='*70}")
        
        # Find examples of both types
        single_day_example = None
        multi_day_example = None
        
        for rec in records:
            if 'messages' in rec and len(rec['messages']) > 0:
                first_msg = rec['messages'][0]
                last_msg = rec['messages'][-1]
                
                first_time = None
                last_time = None
                
                # Check for createdDateTime first
                if 'createdDateTime' in first_msg:
                    first_time = datetime.strptime(first_msg['createdDateTime'], "%Y-%m-%dT%H:%M:%SZ")
                elif 'timestamp' in first_msg:
                    first_time = datetime.strptime(first_msg['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
                elif 'created_at' in first_msg:
                    first_time = datetime.strptime(first_msg['created_at'], "%Y-%m-%dT%H:%M:%SZ")
                
                if 'createdDateTime' in last_msg:
                    last_time = datetime.strptime(last_msg['createdDateTime'], "%Y-%m-%dT%H:%M:%SZ")
                elif 'timestamp' in last_msg:
                    last_time = datetime.strptime(last_msg['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
                elif 'created_at' in last_msg:
                    last_time = datetime.strptime(last_msg['created_at'], "%Y-%m-%dT%H:%M:%SZ")
                
                if first_time and last_time:
                    days_span = (last_time.date() - first_time.date()).days + 1
                    if days_span == 1 and not single_day_example:
                        single_day_example = (rec, first_time, last_time, days_span)
                    elif days_span > 1 and not multi_day_example:
                        multi_day_example = (rec, first_time, last_time, days_span)
            
            if single_day_example and multi_day_example:
                break
        
        if single_day_example:
            rec, first_time, last_time, days_span = single_day_example
            duration = (last_time - first_time).total_seconds() / 60
            print(f"\n📝 Example: Single-Day Conversation")
            print(f"  Started:  {first_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  Ended:    {last_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  Duration: {duration:.1f} minutes")
            print(f"  Messages: {len(rec['messages'])}")
            print(f"  Span:     {days_span} day")
        
        if multi_day_example:
            rec, first_time, last_time, days_span = multi_day_example
            duration = (last_time - first_time).total_seconds() / 60
            print(f"\n📝 Example: Multi-Day Conversation")
            print(f"  Started:  {first_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  Ended:    {last_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  Duration: {duration:.1f} minutes ({duration/60:.1f} hours)")
            print(f"  Messages: {len(rec['messages'])}")
            print(f"  Span:     {days_span} days")
            print(f"  Pattern:  Customer returned after overnight break")

# Run the script
if __name__ == "__main__":
    print("Starting chat date processing from MongoDB...")
    process_chat_records_from_db()
    print("Chat date processing completed!")