"""
consumer_mee.py

Consume json messages from a live data file. 
Insert the processed messages into a database.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

Database functions are in consumers/db_sqlite_case.py.
Environment variables are in utils/utils_config module. 
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import pathlib
import sys
import time
import matplotlib.pyplot as plt
import sqlite3
from datetime import datetime
import pathlib

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger
from .db_sqlite_case import init_db, insert_message
from scipy import interpolate


#####################################
# Function to process a single message
# #####################################

def fetch_keyword_data(sqlite_path):
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    
    # Check if the 'streamed_messages' table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='streamed_messages'")
    if cursor.fetchone() is None:
        logger.error("The 'streamed_messages' table does not exist in the database.")
        conn.close()
        return []

    query = """
    SELECT timestamp, keyword_mentioned
    FROM streamed_messages
    WHERE keyword_mentioned IS NOT NULL
    ORDER BY timestamp
    """
    
    cursor.execute(query)
    data = cursor.fetchall()
    
    conn.close()
    
    return data

def plot_keyword_chart(data):
    keywords = {}
    timestamps = []
    
    for timestamp_str, keyword in data:
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        timestamps.append(timestamp)
        
        if keyword not in keywords:
            keywords[keyword] = [0] * len(timestamps)
        else:
            # Extend the list if necessary
            keywords[keyword].extend([0] * (len(timestamps) - len(keywords[keyword])))
        
        keywords[keyword][-1] += 1
    
    plt.figure(figsize=(12, 6))
    
    for keyword, counts in keywords.items():
        # Use cumulative sum for a running total
        cumulative_counts = [sum(counts[:i+1]) for i in range(len(counts))]
        min_length = min(len(timestamps), len(cumulative_counts))
        plt.plot(timestamps[:min_length], cumulative_counts[:min_length], label=keyword, marker='o')
    
    plt.title("Cumulative Keyword Mentions Over Time")
    plt.xlabel("Timestamp")
    plt.ylabel("Cumulative Keyword Count")
    plt.legend()
    plt.grid(True)
    
    plt.tight_layout()
    plt.show()


def main():
    logger.info("Starting Keyword Tracker")
    
    try:
        sqlite_path = config.get_sqlite_path()
        logger.info(f"Using SQLite database at: {sqlite_path}")
        
        data = fetch_keyword_data(sqlite_path)
        logger.info(f"Fetched {len(data)} data points")
        
        if data:
            plot_keyword_chart(data)
            logger.info("Successfully plotted keyword chart")
        else:
            logger.warning("No keyword data found in the database")
    
    except Exception as e:
        logger.error(f"Error in Keyword Tracker: {e}")

if __name__ == "__main__":
    main()