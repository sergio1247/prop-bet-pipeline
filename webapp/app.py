from flask import Flask, render_template, jsonify, request
import requests
import psycopg2
import json
from datetime import datetime, timedelta
import time
import uuid

app = Flask(__name__)

# Database configuration
import os
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5433'),
    'database': os.getenv('DB_NAME', 'sportsdb'),
    'user': os.getenv('DB_USER', 'sergio'),
    'password': os.getenv('DB_PASSWORD', 'mypassword')
}

# Kafka REST API configuration
KAFKA_REST_URL = 'http://localhost:8082'

# Session tracking for fresh picks
CURRENT_SESSION = None
SESSION_START_TIME = None

def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(**DB_CONFIG)

# Display-only webapp - no manual triggering needed

def get_todays_locks():
    """Get first prediction from database for testing"""
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Simple query to get unique predictions
        query = """
        SELECT DISTINCT
            p.firstname,
            p.lastname,
            pr.stat_type,
            pr.line,
            pr.predicted_value,
            pr.bet_outcome,
            pr.created_at,
            pr.deviation_percentage
        FROM predictions pr
        JOIN players p ON pr.personid = p.personid
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        locks = []
        for row in results:
            locks.append({
                'player_name': f"{row[0]} {row[1]}",
                'stat_type': row[2],
                'line': float(row[3]),
                'predicted_value': float(row[4]),
                'bet_outcome': row[5].upper(),
                'created_at': row[6].strftime('%Y-%m-%d %H:%M:%S') if row[6] else None,
                'deviation_percentage': round(float(row[7]), 1) if row[7] else 0.0,
                'confidence': 'HIGH' if row[7] and float(row[7]) >= 20 else 'MEDIUM'
            })
        
        cursor.close()
        conn.close()
        
        return locks
        
    except Exception as e:
        print(f"Database error: {e}")
        return []

def get_recent_predictions():
    """Get all recent predictions for debugging"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT 
            p.firstname,
            p.lastname,
            pr.stat_type,
            pr.line,
            pr.predicted_value,
            pr.bet_outcome,
            pr.created_at,
            CASE 
                WHEN pr.predicted_value > pr.line 
                THEN ((pr.predicted_value - pr.line) / pr.line) * 100
                ELSE ((pr.line - pr.predicted_value) / pr.line) * 100
            END as deviation_percentage
        FROM predictions pr
        JOIN players p ON pr.personid = p.personid
        ORDER BY pr.created_at DESC
        LIMIT 20
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        predictions = []
        for row in results:
            predictions.append({
                'player_name': f"{row[0]} {row[1]}",
                'stat_type': row[2],
                'line': float(row[3]),
                'predicted_value': float(row[4]),
                'bet_outcome': row[5].upper(),
                'created_at': row[6].strftime('%Y-%m-%d %H:%M:%S') if row[6] else None,
                'deviation_percentage': round(float(row[7]), 1)
            })
        
        cursor.close()
        conn.close()
        
        return predictions
        
    except Exception as e:
        print(f"Database error: {e}")
        return []

@app.route('/')
def index():
    """Main page"""
    return render_template('index.html')

# No manual triggering - runs automatically via Airflow at midnight

@app.route('/get_locks')
def get_locks():
    """API endpoint to get today's locks (>10% deviation)"""
    locks = get_todays_locks()
    return jsonify({
        'success': True,
        'locks': locks,
        'count': len(locks)
    })

@app.route('/get_recent_predictions')
def get_recent():
    """API endpoint to get recent predictions for debugging"""
    predictions = get_recent_predictions()
    return jsonify({
        'success': True,
        'predictions': predictions,
        'count': len(predictions)
    })

@app.route('/status')
def status():
    """Check system status"""
    try:
        # Test database connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM predictions")
        total_predictions = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        # Test Kafka connection
        kafka_response = requests.get(f"{KAFKA_REST_URL}/topics")
        kafka_status = "Connected" if kafka_response.status_code == 200 else "Error"
        
        return jsonify({
            'database': 'Connected',
            'kafka': kafka_status,
            'total_predictions': total_predictions,
            'status': 'Operational'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'Error',
            'error': str(e)
        })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)