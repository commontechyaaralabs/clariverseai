import re
import json
from datetime import datetime, timedelta
import statistics
import os

def parse_log_file(file_path):
    """Parse the log file and extract ticket generation data"""
    timestamps = []
    generation_times = []
    ticket_data = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line_num, line in enumerate(file, 1):
                try:
                    # Parse timestamp from log line
                    timestamp_match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})', line)
                    if timestamp_match:
                        timestamp_str = timestamp_match.group(1)
                        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
                        
                        # Extract JSON data
                        json_start = line.find('{')
                        if json_start != -1:
                            json_str = line[json_start:].strip()
                            try:
                                data = json.loads(json_str)
                                if 'generation_time' in data:
                                    timestamps.append(timestamp)
                                    generation_times.append(data['generation_time'])
                                    ticket_data.append(data)
                            except json.JSONDecodeError as e:
                                print(f"JSON parse error on line {line_num}: {e}")
                                continue
                                
                except Exception as e:
                    print(f"Error processing line {line_num}: {e}")
                    continue
                    
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found!")
        return None, None, None
    except Exception as e:
        print(f"Error reading file: {e}")
        return None, None, None
    
    return timestamps, generation_times, ticket_data

def calculate_intervals(timestamps):
    """Calculate time intervals between consecutive tickets"""
    intervals = []
    for i in range(1, len(timestamps)):
        interval = (timestamps[i] - timestamps[i-1]).total_seconds()
        intervals.append(interval)
    return intervals

def analyze_priorities(ticket_data):
    """Analyze ticket priorities and urgency"""
    priority_counts = {}
    urgent_count = 0
    status_counts = {}
    
    for ticket in ticket_data:
        priority = ticket.get('priority', 'Unknown')
        urgency = ticket.get('urgency', False)
        status = ticket.get('resolution_status', 'Unknown')
        
        priority_counts[priority] = priority_counts.get(priority, 0) + 1
        if urgency:
            urgent_count += 1
        status_counts[status] = status_counts.get(status, 0) + 1
    
    return priority_counts, urgent_count, status_counts

def format_duration(seconds):
    """Format duration in a human-readable format"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    elif minutes > 0:
        return f"{minutes}m {secs}s"
    else:
        return f"{secs}s"

def main():
    # File path - update this to your actual file path
    file_path = r"E:\office\clariverseai\backend\data-type\EU-bank\formatting\ticket\for_new\logs\successful_generations_20250922_101616.log"
    
    print("🎫 TICKET GENERATION LOG ANALYZER")
    print("=" * 50)
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"❌ Error: File not found at {file_path}")
        print("\nPlease check the file path and try again.")
        return
    
    print(f"📂 Reading log file: {os.path.basename(file_path)}")
    
    # Parse the log file
    timestamps, generation_times, ticket_data = parse_log_file(file_path)
    
    if not timestamps:
        print("❌ No valid ticket data found in the log file!")
        return
    
    print(f"✅ Successfully parsed {len(timestamps)} tickets")
    print()
    
    # Calculate intervals
    intervals = calculate_intervals(timestamps)
    
    # Basic statistics
    print("📊 BASIC STATISTICS")
    print("-" * 30)
    print(f"Total tickets analyzed: {len(timestamps)}")
    print(f"Time span: {timestamps[0].strftime('%Y-%m-%d %H:%M:%S')} to {timestamps[-1].strftime('%Y-%m-%d %H:%M:%S')}")
    
    total_duration = (timestamps[-1] - timestamps[0]).total_seconds()
    print(f"Total duration: {format_duration(total_duration)}")
    print(f"Average rate: {len(timestamps) / (total_duration / 3600):.1f} tickets/hour")
    print()
    
    # Interval analysis
    print("⏱️  INTERVAL ANALYSIS")
    print("-" * 30)
    if intervals:
        print(f"Average interval: {statistics.mean(intervals):.1f} seconds")
        print(f"Median interval: {statistics.median(intervals):.1f} seconds")
        print(f"Min interval: {min(intervals):.1f} seconds")
        print(f"Max interval: {max(intervals):.1f} seconds")
        print(f"Standard deviation: {statistics.stdev(intervals):.1f} seconds")
    print()
    
    # Generation time analysis
    print("🔧 GENERATION TIME ANALYSIS")
    print("-" * 30)
    print(f"Average generation time: {statistics.mean(generation_times):.1f} seconds")
    print(f"Median generation time: {statistics.median(generation_times):.1f} seconds")
    print(f"Min generation time: {min(generation_times):.1f} seconds")
    print(f"Max generation time: {max(generation_times):.1f} seconds")
    print(f"Standard deviation: {statistics.stdev(generation_times):.1f} seconds")
    print()
    
    # Priority and urgency analysis
    priority_counts, urgent_count, status_counts = analyze_priorities(ticket_data)
    
    print("🚨 PRIORITY & STATUS ANALYSIS")
    print("-" * 30)
    print("Priority distribution:")
    for priority, count in sorted(priority_counts.items()):
        percentage = (count / len(ticket_data)) * 100
        print(f"  {priority}: {count} ({percentage:.1f}%)")
    
    print(f"\nUrgent tickets: {urgent_count} ({(urgent_count/len(ticket_data)*100):.1f}%)")
    
    print("\nStatus distribution:")
    for status, count in sorted(status_counts.items()):
        percentage = (count / len(ticket_data)) * 100
        print(f"  {status}: {count} ({percentage:.1f}%)")
    print()
    
    # Estimates for 2000 tickets
    print("🎯 ESTIMATES FOR 2000 TICKETS")
    print("=" * 40)
    
    if intervals:
        avg_interval = statistics.mean(intervals)
        median_interval = statistics.median(intervals)
        
        # Current rate
        current_rate = len(timestamps) / (total_duration / 3600)  # tickets per hour
        
        print(f"Current generation rate: {current_rate:.1f} tickets/hour")
        print()
        
        # Method 1: Using current rate
        time_by_rate = 2000 / current_rate * 3600  # in seconds
        print(f"📈 Method 1 - Based on current rate:")
        print(f"   Time needed: {format_duration(time_by_rate)} ({time_by_rate/3600:.1f} hours)")
        print()
        
        # Method 2: Using average interval
        time_avg_interval = (2000 - 1) * avg_interval
        print(f"📊 Method 2 - Based on average interval ({avg_interval:.1f}s):")
        print(f"   Time needed: {format_duration(time_avg_interval)} ({time_avg_interval/3600:.1f} hours)")
        print()
        
        # Method 3: Using median interval
        time_median_interval = (2000 - 1) * median_interval
        print(f"📊 Method 3 - Based on median interval ({median_interval:.1f}s):")
        print(f"   Time needed: {format_duration(time_median_interval)} ({time_median_interval/3600:.1f} hours)")
        print()
        
        # Conservative estimate
        conservative_multiplier = 1.3  # 30% buffer
        conservative_time = time_avg_interval * conservative_multiplier
        print(f"🛡️  Conservative estimate (+30% buffer):")
        print(f"   Time needed: {format_duration(conservative_time)} ({conservative_time/3600:.1f} hours)")
        print()
        
        # Best and worst case scenarios
        best_case = (2000 - 1) * min(intervals)
        worst_case = (2000 - 1) * max(intervals)
        print(f"⚡ Best case scenario (min interval): {format_duration(best_case)} ({best_case/3600:.1f} hours)")
        print(f"🐌 Worst case scenario (max interval): {format_duration(worst_case)} ({worst_case/3600:.1f} hours)")
        print()
        
        # Recommended estimate
        recommended = statistics.median([time_by_rate, time_avg_interval, conservative_time])
        print("🎯 RECOMMENDED ESTIMATE")
        print("=" * 25)
        print(f"⭐ Expected time for 2000 tickets: {format_duration(recommended)} ({recommended/3600:.1f} hours)")
        print()
        
        print("💡 RECOMMENDATIONS:")
        print("- Monitor system performance during generation")
        print("- Consider running during off-peak hours")
        print("- Have system monitoring in place")
        print("- Plan for potential slowdowns as ticket count increases")
        
        if urgent_count > 0:
            urgent_percentage = (urgent_count/len(ticket_data)*100)
            print(f"- Expect ~{urgent_percentage:.1f}% of tickets to be urgent priority")

if __name__ == "__main__":
    main()