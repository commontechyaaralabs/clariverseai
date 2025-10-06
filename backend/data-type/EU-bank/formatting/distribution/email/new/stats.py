import json
from datetime import datetime
from collections import defaultdict, Counter

def parse_date(date_string):
    """Parse ISO date string to datetime object"""
    try:
        return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ")
    except:
        try:
            return datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        except:
            return None

def generate_date_stats(file_path):
    """
    Generate comprehensive date statistics for email records
    
    Args:
        file_path: Path to the JSON file containing email records
    """
    print(f"Reading from: {file_path}\n")
    
    # Read the file
    with open(file_path, 'r', encoding='utf-8') as f:
        records = json.load(f)
    
    print(f"Total records: {len(records)}\n")
    
    # Collect dates
    date_counts = defaultdict(int)
    month_counts = defaultdict(int)
    
    for record in records:
        first_message_date_str = record['thread']['first_message_at']
        first_message_date = parse_date(first_message_date_str)
        
        if first_message_date:
            # Count by date (YYYY-MM-DD)
            date_key = first_message_date.strftime("%Y-%m-%d")
            date_counts[date_key] += 1
            
            # Count by month (YYYY-MM)
            month_key = first_message_date.strftime("%Y-%m")
            month_counts[month_key] += 1
    
    # Sort dates
    sorted_dates = sorted(date_counts.items())
    sorted_months = sorted(month_counts.items())
    
    # === STATISTICS SUMMARY ===
    print("="*70)
    print("OVERALL STATISTICS")
    print("="*70)
    
    total_days = len(date_counts)
    total_emails = sum(date_counts.values())
    days_with_emails = sum(1 for count in date_counts.values() if count > 0)
    days_without_emails = 181 - days_with_emails  # 6 months ≈ 181 days
    
    print(f"Total emails:           {total_emails}")
    print(f"Days with emails:       {days_with_emails}")
    print(f"Days without emails:    {days_without_emails}")
    print(f"Average per day:        {total_emails / max(total_days, 1):.2f}")
    print(f"Max emails in one day:  {max(date_counts.values())}")
    print(f"Min emails in one day:  {min(date_counts.values())}")
    
    # === MONTHLY BREAKDOWN ===
    print("\n" + "="*70)
    print("MONTHLY BREAKDOWN")
    print("="*70)
    print(f"{'Month':<15} {'Emails':<10} {'Avg/Day':<10}")
    print("-"*70)
    
    month_names = {
        '2025-01': 'January 2025',
        '2025-02': 'February 2025',
        '2025-03': 'March 2025',
        '2025-04': 'April 2025',
        '2025-05': 'May 2025',
        '2025-06': 'June 2025'
    }
    
    days_in_month = {
        '2025-01': 31,
        '2025-02': 28,
        '2025-03': 31,
        '2025-04': 30,
        '2025-05': 31,
        '2025-06': 30
    }
    
    for month, count in sorted_months:
        month_name = month_names.get(month, month)
        days = days_in_month.get(month, 30)
        avg_per_day = count / days
        print(f"{month_name:<15} {count:<10} {avg_per_day:<10.2f}")
    
    # === DAILY BREAKDOWN ===
    print("\n" + "="*70)
    print("DAILY BREAKDOWN (Date - Number of Records)")
    print("="*70)
    print(f"{'Date':<15} {'Day':<12} {'Count':<8} {'Bar Chart'}")
    print("-"*70)
    
    max_count = max(date_counts.values())
    
    for date_str, count in sorted_dates:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        day_name = date_obj.strftime("%A")
        
        # Create a simple bar chart
        bar_length = int((count / max_count) * 40)
        bar = "█" * bar_length
        
        print(f"{date_str:<15} {day_name:<12} {count:<8} {bar}")
    
    # === TOP 10 BUSIEST DAYS ===
    print("\n" + "="*70)
    print("TOP 10 BUSIEST DAYS")
    print("="*70)
    print(f"{'Rank':<6} {'Date':<15} {'Day':<12} {'Count'}")
    print("-"*70)
    
    sorted_by_count = sorted(date_counts.items(), key=lambda x: x[1], reverse=True)
    for rank, (date_str, count) in enumerate(sorted_by_count[:10], 1):
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        day_name = date_obj.strftime("%A")
        print(f"{rank:<6} {date_str:<15} {day_name:<12} {count}")
    
    # === TOP 10 QUIETEST DAYS ===
    print("\n" + "="*70)
    print("TOP 10 QUIETEST DAYS")
    print("="*70)
    print(f"{'Rank':<6} {'Date':<15} {'Day':<12} {'Count'}")
    print("-"*70)
    
    sorted_by_count_asc = sorted(date_counts.items(), key=lambda x: x[1])
    for rank, (date_str, count) in enumerate(sorted_by_count_asc[:10], 1):
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        day_name = date_obj.strftime("%A")
        print(f"{rank:<6} {date_str:<15} {day_name:<12} {count}")
    
    # === DAY OF WEEK ANALYSIS ===
    print("\n" + "="*70)
    print("DAY OF WEEK ANALYSIS")
    print("="*70)
    
    day_of_week_counts = defaultdict(int)
    for date_str, count in sorted_dates:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        day_name = date_obj.strftime("%A")
        day_of_week_counts[day_name] += count
    
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    print(f"{'Day':<12} {'Total Emails':<15} {'Percentage'}")
    print("-"*70)
    
    for day in day_order:
        count = day_of_week_counts.get(day, 0)
        percentage = (count / total_emails * 100) if total_emails > 0 else 0
        print(f"{day:<12} {count:<15} {percentage:.2f}%")
    
    print("\n" + "="*70)
    print("STATISTICS GENERATION COMPLETE")
    print("="*70)
    
    # === EXPORT TO CSV (Optional) ===
    print("\n📊 Exporting daily stats to CSV...")
    csv_filename = file_path.replace('.json', '_daily_stats.csv')
    
    with open(csv_filename, 'w', encoding='utf-8') as csv_file:
        csv_file.write("Date,Day,Email Count\n")
        for date_str, count in sorted_dates:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            day_name = date_obj.strftime("%A")
            csv_file.write(f"{date_str},{day_name},{count}\n")
    
    print(f"✓ Daily stats exported to: {csv_filename}")
    
    # === EXPORT MONTHLY STATS TO CSV ===
    monthly_csv_filename = file_path.replace('.json', '_monthly_stats.csv')
    
    with open(monthly_csv_filename, 'w', encoding='utf-8') as csv_file:
        csv_file.write("Month,Email Count,Average Per Day\n")
        for month, count in sorted_months:
            month_name = month_names.get(month, month)
            days = days_in_month.get(month, 30)
            avg_per_day = count / days
            csv_file.write(f"{month_name},{count},{avg_per_day:.2f}\n")
    
    print(f"✓ Monthly stats exported to: {monthly_csv_filename}")

# Run the script
if __name__ == "__main__":
    # Your file name
    file_path = "output_emails2.json"
    
    generate_date_stats(file_path)