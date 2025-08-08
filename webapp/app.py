from flask import Flask, render_template, jsonify, request
import requests
import psycopg2
import json
from datetime import datetime, timedelta
import time
import uuid
import sys
from decimal import Decimal

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
        print("ENTERING get_todays_locks()")
        sys.stdout.flush()
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Simple query to get unique predictions with all Monte Carlo columns, sorted by highest deviation
        query = """
        SELECT DISTINCT
            p.firstname,
            p.lastname,
            pr.stat_type,
            pr.line,
            pr.predicted_value,
            pr.bet_outcome,
            pr.created_at,
            pr.deviation_percentage,
            pr.team,
            pr.probability_over,
            pr.confidence_low,
            pr.confidence_high,
            pr.confidence_score,
            pr.risk_assessment
        FROM predictions pr
        JOIN players p ON pr.personid = p.personid
        ORDER BY pr.deviation_percentage DESC NULLS LAST
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        print(f"FETCHED {len(results)} rows")
        
        locks = []
        for row in results:
            try:
                # Calculate correct bet outcome based on prediction vs line
                predicted_value = float(row[4])
                line = float(row[3])
                bet_outcome = 'OVER' if predicted_value > line else 'UNDER'
                
                locks.append({
                    'player_name': f"{row[0]} {row[1]}",
                    'stat_type': row[2],
                    'line': line,
                    'predicted_value': predicted_value,
                    'bet_outcome': bet_outcome,
                    'created_at': row[6].strftime('%Y-%m-%d %H:%M:%S') if row[6] else None,
                    'deviation_percentage': round(float(row[7]), 1) if row[7] else 0.0,
                    'team': row[8] if row[8] else 'N/A',
                    'confidence': 'HIGH' if row[7] and float(row[7]) >= 20 else 'MEDIUM'
                })
            except Exception as e:
                print(f"ERROR processing row {row}: {e}")
                print(f"Row length: {len(row)}")
                sys.stdout.flush()
                # Fallback to basic data only
                predicted_value = float(row[4])
                line = float(row[3])
                bet_outcome = 'OVER' if predicted_value > line else 'UNDER'
                
                locks.append({
                    'player_name': f"{row[0]} {row[1]}",
                    'stat_type': row[2],
                    'line': line,
                    'predicted_value': predicted_value,
                    'bet_outcome': bet_outcome,
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

def get_processing_metrics():
    """Get processing metrics from the most recent prediction run"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get the most recent run's timing data
        query = """
        SELECT 
            COUNT(*) as total_predictions,
            MIN(created_at) as run_start,
            MAX(created_at) as run_end,
            MAX(processing_time_seconds) as monte_carlo_seconds,
            MAX(total_processing_time_seconds) as total_processing_seconds,
            MAX(created_at) as last_run_time
        FROM predictions 
        WHERE created_at >= (
            SELECT MAX(created_at) - INTERVAL '5 minutes' 
            FROM predictions
        )
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result and result[0] > 0:
            # Handle Decimal type from database
            monte_carlo_time = float(result[3]) if result[3] is not None else 0
            total_processing_time = float(result[4]) if result[4] is not None else 0
            return {
                'total_predictions': result[0],
                'run_start': result[1].strftime('%H:%M:%S') if result[1] else None,
                'run_end': result[2].strftime('%H:%M:%S') if result[2] else None, 
                'monte_carlo_seconds': round(monte_carlo_time, 2),
                'total_processing_seconds': round(total_processing_time, 2),
                'last_run_time': result[5].strftime('%Y-%m-%d %H:%M:%S') if result[5] else None,
                'monte_carlo_simulations': result[0] * 500000 if result[0] else 0,  # 500k sims per prediction
                'throughput': round((result[0] * 500000) / max(total_processing_time, 1), 0) if total_processing_time > 0 else 0
            }
        else:
            return None
            
    except Exception as e:
        print(f"Error getting processing metrics: {e}")
        return None

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
            pr.deviation_percentage,
            pr.team,
            pr.probability_over,
            pr.confidence_low,
            pr.confidence_high,
            pr.confidence_score,
            pr.risk_assessment
        FROM predictions pr
        JOIN players p ON pr.personid = p.personid
        ORDER BY pr.deviation_percentage DESC NULLS LAST, pr.created_at DESC
        LIMIT 20
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        predictions = []
        for row in results:
            # Calculate correct bet outcome based on prediction vs line
            predicted_value = float(row[4])
            line = float(row[3])
            bet_outcome = 'OVER' if predicted_value > line else 'UNDER'
            
            predictions.append({
                'player_name': f"{row[0]} {row[1]}",
                'stat_type': row[2],
                'line': line,
                'predicted_value': predicted_value,
                'bet_outcome': bet_outcome,
                'created_at': row[6].strftime('%Y-%m-%d %H:%M:%S') if row[6] else None,
                'deviation_percentage': round(float(row[7]), 1) if row[7] else 0.0,
                'team': row[8] if row[8] else 'N/A'
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
    
    # Debug: check first lock for new columns
    if locks:
        first_lock = locks[0]
        print(f"DEBUG: First lock keys: {list(first_lock.keys())}")
        print(f"DEBUG: Has team: {'team' in first_lock}")
        print(f"DEBUG: Has probability_over: {'probability_over' in first_lock}")
    
    return jsonify({
        'success': True,
        'locks': locks,
        'count': len(locks)
    })

@app.route('/get_metrics')
def get_metrics():
    """API endpoint to get processing metrics from most recent run"""
    metrics = get_processing_metrics()
    return jsonify({
        'success': True,
        'metrics': metrics
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