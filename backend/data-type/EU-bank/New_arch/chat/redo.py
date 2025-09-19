# example_usage.py - Example usage of the Chat Analysis System

import logging
from chat_analyzer import ChatMessageAnalyzer
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_specific_records():
    """Example: Analyze specific records based on criteria"""
    analyzer = ChatMessageAnalyzer()
    
    try:
        # Analyze records where sentiment is missing
        query = {
            '$or': [
                {'sentiment': {'$exists': False}},
                {'overall_sentiment': None}
            ]
        }
        
        logger.info("Analyzing records with missing sentiment data")
        analyzer.run_analysis(limit=10, query=query)
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
    finally:
        analyzer.close()

def analyze_recent_records():
    """Example: Analyze recent records only"""
    analyzer = ChatMessageAnalyzer()
    
    try:
        # Analyze records from the last 30 days
        from datetime import datetime, timedelta
        thirty_days_ago = datetime.utcnow() - timedelta(days=30)
        
        query = {
            '$and': [
                {
                    '$or': [
                        {'sentiment': {'$exists': False}},
                        {'chat_summary': {'$exists': False}}
                    ]
                },
                {
                    'processed_at': {
                        '$gte': thirty_days_ago.isoformat()
                    }
                }
            ]
        }
        
        logger.info("Analyzing recent records with missing analysis")
        analyzer.run_analysis(query=query)
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
    finally:
        analyzer.close()

def test_single_record():
    """Example: Test analysis on a single record"""
    analyzer = ChatMessageAnalyzer()
    
    try:
        # Get one record for testing
        record = analyzer.chat_col.find_one({
            '$or': [
                {'sentiment': {'$exists': False}},
                {'overall_sentiment': None}
            ]
        })
        
        if record:
            logger.info(f"Testing analysis on record: {record['_id']}")
            
            # Process the record
            result = analyzer.process_chat_record(record)
            
            if result:
                # Update the database
                success = analyzer.update_database_record(result)
                if success:
                    logger.info("Test completed successfully")
                else:
                    logger.error("Failed to update database")
            else:
                logger.error("Failed to process record")
        else:
            logger.info("No records found that need analysis")
            
    except Exception as e:
        logger.error(f"Test failed: {e}")
    finally:
        analyzer.close()

def analyze_by_topic():
    """Example: Analyze records by specific topic"""
    analyzer = ChatMessageAnalyzer()
    
    try:
        # Analyze records with specific banking topics
        query = {
            '$and': [
                {
                    '$or': [
                        {'sentiment': {'$exists': False}},
                        {'chat_summary': {'$exists': False}}
                    ]
                },
                {
                    '$or': [
                        {'dominant_topic': {'$regex': 'Financial', '$options': 'i'}},
                        {'chat.topic': {'$regex': 'Reporting', '$options': 'i'}},
                        {'subtopics': {'$regex': 'Compliance', '$options': 'i'}}
                    ]
                }
            ]
        }
        
        logger.info("Analyzing records with financial/compliance topics")
        analyzer.run_analysis(query=query)
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
    finally:
        analyzer.close()

def reanalyze_existing_records():
    """Example: Re-analyze records that already have analysis but might need updates"""
    analyzer = ChatMessageAnalyzer()
    
    try:
        # Re-analyze records that have analysis but might be outdated
        query = {
            '$and': [
                {'sentiment': {'$exists': True}},
                {'overall_sentiment': {'$exists': True}},
                {
                    '$or': [
                        {'analysis_version': {'$exists': False}},
                        {'analysis_version': {'$ne': '2.0'}},
                        {'processed_at': {'$lt': '2025-01-01T00:00:00'}}
                    ]
                }
            ]
        }
        
        logger.info("Re-analyzing existing records with outdated analysis")
        analyzer.run_analysis(limit=50, query=query)
        
    except Exception as e:
        logger.error(f"Re-analysis failed: {e}")
    finally:
        analyzer.close()

def analyze_urgent_records():
    """Example: Priority analysis for potentially urgent records"""
    analyzer = ChatMessageAnalyzer()
    
    try:
        # Focus on records that might be urgent
        query = {
            '$and': [
                {
                    '$or': [
                        {'urgency': {'$exists': False}},
                        {'stages': {'$exists': False}}
                    ]
                },
                {
                    '$or': [
                        {'dominant_topic': {'$regex': 'Complaint|Issue|Problem', '$options': 'i'}},
                        {'subtopics': {'$regex': 'Fraud|Security|Emergency', '$options': 'i'}},
                        {'chat.topic': {'$regex': 'Urgent|Critical|Emergency', '$options': 'i'}}
                    ]
                }
            ]
        }
        
        logger.info("Analyzing potentially urgent records")
        analyzer.run_analysis(query=query)
        
    except Exception as e:
        logger.error(f"Urgent analysis failed: {e}")
    finally:
        analyzer.close()

def get_analysis_statistics():
    """Example: Get statistics about analysis completion"""
    analyzer = ChatMessageAnalyzer()
    
    try:
        # Get total counts
        total_records = analyzer.chat_col.count_documents({})
        
        # Records with sentiment analysis
        with_sentiment = analyzer.chat_col.count_documents({
            'sentiment': {'$exists': True, '$ne': None}
        })
        
        # Records with overall sentiment
        with_overall_sentiment = analyzer.chat_col.count_documents({
            'overall_sentiment': {'$exists': True, '$ne': None}
        })
        
        # Records with chat summary
        with_summary = analyzer.chat_col.count_documents({
            'chat_summary': {'$exists': True, '$ne': None, '$ne': ''}
        })
        
        # Records with complete analysis
        complete_analysis = analyzer.chat_col.count_documents({
            '$and': [
                {'sentiment': {'$exists': True, '$ne': None}},
                {'overall_sentiment': {'$exists': True, '$ne': None}},
                {'chat_summary': {'$exists': True, '$ne': None, '$ne': ''}},
                {'stages': {'$exists': True, '$ne': None}},
                {'resolution_status': {'$exists': True, '$ne': None}}
            ]
        })
        
        # Records needing analysis
        needs_analysis = analyzer.chat_col.count_documents({
            '$or': [
                {'sentiment': {'$exists': False}},
                {'overall_sentiment': {'$exists': False}},
                {'chat_summary': {'$exists': False}},
                {'sentiment': None},
                {'overall_sentiment': None},
                {'chat_summary': None},
                {'chat_summary': ''}
            ]
        })
        
        logger.info("=== ANALYSIS STATISTICS ===")
        logger.info(f"Total records: {total_records}")
        logger.info(f"Records with sentiment: {with_sentiment} ({(with_sentiment/total_records)*100:.1f}%)")
        logger.info(f"Records with overall sentiment: {with_overall_sentiment} ({(with_overall_sentiment/total_records)*100:.1f}%)")
        logger.info(f"Records with summary: {with_summary} ({(with_summary/total_records)*100:.1f}%)")
        logger.info(f"Records with complete analysis: {complete_analysis} ({(complete_analysis/total_records)*100:.1f}%)")
        logger.info(f"Records needing analysis: {needs_analysis} ({(needs_analysis/total_records)*100:.1f}%)")
        
        # Get sample of urgent records
        urgent_records = analyzer.chat_col.count_documents({'urgency': True})
        logger.info(f"Records marked as urgent: {urgent_records}")
        
        # Get distribution of stages
        stages_pipeline = [
            {'$group': {'_id': '$stages', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}}
        ]
        stages_dist = list(analyzer.chat_col.aggregate(stages_pipeline))
        logger.info("Stages distribution:")
        for stage in stages_dist:
            if stage['_id']:
                logger.info(f"  {stage['_id']}: {stage['count']}")
        
    except Exception as e:
        logger.error(f"Statistics query failed: {e}")
    finally:
        analyzer.close()

def main():
    """Main function with different analysis options"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python example_usage.py [option]")
        print("Options:")
        print("  test        - Test analysis on a single record")
        print("  missing     - Analyze records with missing sentiment data")
        print("  recent      - Analyze recent records")
        print("  topic       - Analyze by specific topics")
        print("  reanalyze   - Re-analyze existing records")
        print("  urgent      - Analyze potentially urgent records")
        print("  stats       - Show analysis statistics")
        print("  all         - Analyze all records needing analysis")
        return
    
    option = sys.argv[1].lower()
    
    if option == "test":
        test_single_record()
    elif option == "missing":
        analyze_specific_records()
    elif option == "recent":
        analyze_recent_records()
    elif option == "topic":
        analyze_by_topic()
    elif option == "reanalyze":
        reanalyze_existing_records()
    elif option == "urgent":
        analyze_urgent_records()
    elif option == "stats":
        get_analysis_statistics()
    elif option == "all":
        # Run full analysis
        analyzer = ChatMessageAnalyzer()
        try:
            analyzer.run_analysis()
        finally:
            analyzer.close()
    else:
        print(f"Unknown option: {option}")

if __name__ == "__main__":
    main()