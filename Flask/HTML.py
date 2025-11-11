import sys
import os
from flask import Flask, render_template, url_for, redirect, request, jsonify
import sqlite3
import json
from datetime import datetime

# Add the path to the parent directory to the sys.path list
sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))

app = Flask(__name__)

# Database configuration
SQLITE_DB = os.path.join(os.path.dirname(__file__), '..', 'travel.sqlite')

def get_db_connection():
    """Get SQLite database connection"""
    conn = sqlite3.connect(SQLITE_DB)
    conn.row_factory = sqlite3.Row
    return conn

def extract_json_value(json_str):
    """Extract value from JSON string (for model, city, airport_name fields)"""
    if not json_str:
        return "Unknown"
    try:
        data = json.loads(json_str)
        # Try to get English value or first available value
        if isinstance(data, dict):
            return data.get('en', list(data.values())[0] if data else 'Unknown')
        return str(data)
    except:
        return str(json_str)

# Route for the home page
@app.route('/')
def index():
    return render_template('index.html')

# Route to the attributes page
@app.route('/attributes')
def attributes():
    return render_template('attributes.html')

# ==================== DASHBOARD API ENDPOINTS ====================

@app.route('/api/flight-operations')
def flight_operations():
    """Get flight operations metrics (scheduled vs completed, cancellations, delays)"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Create view for flight routes
    cursor.execute("DROP VIEW IF EXISTS flight_routes;")
    cursor.execute("""
        CREATE VIEW flight_routes AS
        SELECT 
            f.*,
            f.departure_airport || ' → ' || f.arrival_airport AS route
        FROM flights f;""")

    # Example: query the new view
    cursor.execute("SELECT route, COUNT(*) as total FROM flight_routes GROUP BY route LIMIT 5;")
    # cursor.execute("SELECT DISTINCT route, COUNT(*) as total FROM flight_routes GROUP BY route;")
    flight_routes = [dict(row) for row in cursor.fetchall()]
    print("\n=== Sample flight_routes data ===")
    for row in flight_routes:
        print(row)
    
    # Top 5 Least Punctual Routes
    cursor.execute("""
        SELECT 
            route,
            -- Strip the '+HH' or '+HH:MM' timezone offset before passing to JULIANDAY
            ROUND(AVG(
                (
                    JULIANDAY(SUBSTR(actual_arrival, 1, 19)) - 
                    JULIANDAY(SUBSTR(scheduled_arrival, 1, 19))
                ) * 24 * 60
            ), 2) AS avg_delay_mins
        FROM flight_routes
        WHERE status = 'Arrived'
            AND actual_arrival IS NOT NULL
            AND scheduled_arrival IS NOT NULL
            -- Use the stripped timestamp for comparison to ensure valid JULIANDAY call
            AND JULIANDAY(SUBSTR(actual_arrival, 1, 19)) > JULIANDAY(SUBSTR(scheduled_arrival, 1, 19))
        GROUP BY route
        HAVING avg_delay_mins > 0
        ORDER BY avg_delay_mins DESC
        LIMIT 5;
    """)
    least_punctual_routes = [dict(row) for row in cursor.fetchall()]
    
    # Overall flight metrics
    cursor.execute("""
        SELECT 
            COUNT(*) as total_flights,
            SUM(CASE WHEN status LIKE '%Delayed%' OR status = 'Delayed' THEN 1 ELSE 0 END) as delayed_flights,
            SUM(CASE WHEN status LIKE '%Cancel%' OR status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled_flights,
            SUM(CASE WHEN status = 'Arrived' OR status = 'On Time' THEN 1 ELSE 0 END) as ontime_flights,
            ROUND(AVG(CASE 
                WHEN actual_departure IS NOT NULL AND scheduled_departure IS NOT NULL
                THEN (julianday(actual_departure) - julianday(scheduled_departure)) * 24 * 60 
                ELSE 0 
            END), 2) as avg_delay_minutes
        FROM flights
    """)
    overview = dict(cursor.fetchone())
    
    conn.close()
    return jsonify({
        'least_punctual_routes': least_punctual_routes,
        'overview': overview
    })

@app.route('/api/route-performance')
def route_performance():
    """Get route and airport performance metrics"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Busiest routes
    cursor.execute("""
        SELECT 
            departure_airport || ' → ' || arrival_airport as route,
            COUNT(*) as flight_count,
            ROUND(AVG(CASE 
                WHEN status = 'Arrived' OR status = 'On Time' THEN 1.0 
                ELSE 0.0 
            END) * 100, 2) as ontime_percentage
        FROM flights
        GROUP BY departure_airport, arrival_airport
        ORDER BY flight_count DESC
        LIMIT 10
    """)
    busiest_routes = [dict(row) for row in cursor.fetchall()]
    
    # Busiest airports (departures)
    cursor.execute("""
        SELECT 
            f.departure_airport as airport,
            COUNT(*) as departure_count
        FROM flights f
        GROUP BY f.departure_airport
        ORDER BY departure_count DESC
        LIMIT 10
    """)
    busiest_airports_raw = cursor.fetchall()
    
    # Format airport names with city
    busiest_airports = []
    for row in busiest_airports_raw:
        cursor.execute("""
            SELECT city, airport_name 
            FROM airports_data 
            WHERE airport_code = ?
        """, (row['airport'],))
        airport_info = cursor.fetchone()
        if airport_info:
            city = extract_json_value(airport_info['city'])
            airport_name = extract_json_value(airport_info['airport_name'])
            busiest_airports.append({
                'airport': f"{city} ({row['airport']})",
                'departure_count': row['departure_count']
            })
    
    # On-time performance by route
    cursor.execute("""
        SELECT 
            departure_airport || ' → ' || arrival_airport as route,
            COUNT(*) as total_flights,
            SUM(CASE WHEN status = 'Arrived' OR status = 'On Time' THEN 1 ELSE 0 END) as ontime_flights,
            ROUND(SUM(CASE 
                WHEN status = 'Arrived' OR status = 'On Time' THEN 1.0 
                ELSE 0.0 
            END) / COUNT(*) * 100, 2) as ontime_rate
        FROM flights
        GROUP BY departure_airport, arrival_airport
        HAVING COUNT(*) >= 10
        ORDER BY ontime_rate DESC
        LIMIT 10
    """)
    ontime_performance = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    return jsonify({
        'busiest_routes': busiest_routes,
        'busiest_airports': busiest_airports,
        'ontime_performance': ontime_performance
    })

@app.route('/api/passenger-demand')
def passenger_demand():
    """Get passenger demand and load factors"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Load factor by aircraft
    cursor.execute("""
        SELECT 
            f.aircraft_code,
            COUNT(DISTINCT f.flight_id) as total_flights,
            COUNT(tf.ticket_no) as tickets_sold,
            (SELECT COUNT(*) FROM seats s WHERE s.aircraft_code = f.aircraft_code) as total_seats,
            ROUND(CAST(COUNT(tf.ticket_no) AS FLOAT) / 
                  (COUNT(DISTINCT f.flight_id) * 
                   (SELECT COUNT(*) FROM seats s WHERE s.aircraft_code = f.aircraft_code)) * 100, 2) as load_factor
        FROM flights f
        LEFT JOIN ticket_flights tf ON f.flight_id = tf.flight_id
        GROUP BY f.aircraft_code
        ORDER BY load_factor DESC
    """)
    load_factors_raw = cursor.fetchall()
    
    # Get aircraft model names
    load_factors = []
    for row in load_factors_raw:
        cursor.execute("""
            SELECT model FROM aircrafts_data WHERE aircraft_code = ?
        """, (row['aircraft_code'],))
        model_info = cursor.fetchone()
        if model_info:
            model = extract_json_value(model_info['model'])
            load_factors.append({
                'aircraft_code': row['aircraft_code'],
                'aircraft_model': model,
                'total_flights': row['total_flights'],
                'tickets_sold': row['tickets_sold'],
                'total_seats': row['total_seats'],
                'load_factor': row['load_factor'] if row['load_factor'] else 0
            })
    
    # Fare class distribution
    cursor.execute("""
        SELECT 
            fare_conditions,
            COUNT(*) as ticket_count,
            ROUND(AVG(amount), 2) as avg_price
        FROM ticket_flights
        WHERE fare_conditions IS NOT NULL
        GROUP BY fare_conditions
        ORDER BY ticket_count DESC
    """)
    fare_distribution = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    return jsonify({
        'load_factors': load_factors,
        'fare_distribution': fare_distribution
    })

@app.route('/api/revenue-analysis')
def revenue_analysis():
    """Get revenue distribution metrics"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Revenue by fare class
    cursor.execute("""
        SELECT 
            fare_conditions,
            COUNT(*) as ticket_count,
            ROUND(SUM(amount), 2) as total_revenue,
            ROUND(AVG(amount), 2) as avg_ticket_price
        FROM ticket_flights
        WHERE fare_conditions IS NOT NULL
        GROUP BY fare_conditions
        ORDER BY total_revenue DESC
    """)
    revenue_by_class = [dict(row) for row in cursor.fetchall()]
    
    # Revenue by route (top 10)
    cursor.execute("""
        SELECT 
            f.departure_airport || ' → ' || f.arrival_airport as route,
            COUNT(tf.ticket_no) as tickets_sold,
            ROUND(SUM(tf.amount), 2) as total_revenue,
            ROUND(AVG(tf.amount), 2) as avg_ticket_price
        FROM ticket_flights tf
        JOIN flights f ON tf.flight_id = f.flight_id
        GROUP BY f.departure_airport, f.arrival_airport
        ORDER BY total_revenue DESC
        LIMIT 10
    """)
    revenue_by_route = [dict(row) for row in cursor.fetchall()]
    
    # Monthly revenue trend (last 12 months with data)
    cursor.execute("""
        SELECT 
            strftime('%Y-%m', b.book_date) as month,
            ROUND(SUM(b.total_amount), 2) as revenue,
            COUNT(DISTINCT b.book_ref) as bookings
        FROM bookings b
        WHERE b.book_date IS NOT NULL
        GROUP BY strftime('%Y-%m', b.book_date)
        ORDER BY month DESC
        LIMIT 12
    """)
    monthly_revenue_raw = cursor.fetchall()
    # Reverse to show chronologically
    monthly_revenue = [dict(row) for row in reversed(monthly_revenue_raw)]
    
    conn.close()
    return jsonify({
        'revenue_by_class': revenue_by_class,
        'revenue_by_route': revenue_by_route,
        'monthly_revenue': monthly_revenue
    })

@app.route('/api/resource-planning')
def resource_planning():
    """Get resource planning metrics (aircraft utilisation, turnaround)"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Aircraft utilisation
    cursor.execute("""
        SELECT 
            f.aircraft_code,
            COUNT(*) as total_flights,
            COUNT(DISTINCT DATE(f.scheduled_departure)) as operating_days,
            ROUND(CAST(COUNT(*) AS FLOAT) / 
                  NULLIF(COUNT(DISTINCT DATE(f.scheduled_departure)), 0), 2) as flights_per_day
        FROM flights f
        WHERE f.scheduled_departure IS NOT NULL
        GROUP BY f.aircraft_code
        ORDER BY flights_per_day DESC
    """)
    utilization_raw = cursor.fetchall()
    
    # Get aircraft model names
    aircraft_utilization = []
    for row in utilization_raw:
        cursor.execute("""
            SELECT model FROM aircrafts_data WHERE aircraft_code = ?
        """, (row['aircraft_code'],))
        model_info = cursor.fetchone()
        if model_info:
            model = extract_json_value(model_info['model'])
            aircraft_utilization.append({
                'aircraft_code': row['aircraft_code'],
                'aircraft_model': model,
                'total_flights': row['total_flights'],
                'operating_days': row['operating_days'],
                'flights_per_day': row['flights_per_day'] if row['flights_per_day'] else 0
            })
    
    # Passenger booking patterns
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT passenger_id) as total_passengers,
            COUNT(DISTINCT ticket_no) as total_tickets,
            ROUND(CAST(COUNT(DISTINCT ticket_no) AS FLOAT) / 
                  COUNT(DISTINCT passenger_id), 2) as avg_tickets_per_passenger
        FROM tickets
    """)
    passenger_stats = dict(cursor.fetchone())
    
    # Calculate repeat passengers
    cursor.execute("""
        SELECT 
            COUNT(*) as total_passengers,
            SUM(CASE WHEN ticket_count > 1 THEN 1 ELSE 0 END) as repeat_passengers
        FROM (
            SELECT passenger_id, COUNT(DISTINCT ticket_no) as ticket_count
            FROM tickets
            GROUP BY passenger_id
        )
    """)
    repeat_stats = dict(cursor.fetchone())
    
    passenger_retention = {
        'total_passengers': passenger_stats['total_passengers'],
        'repeat_passengers': repeat_stats['repeat_passengers'],
        'repeat_rate': round((repeat_stats['repeat_passengers'] / passenger_stats['total_passengers'] * 100), 2) if passenger_stats['total_passengers'] > 0 else 0,
        'avg_tickets_per_passenger': passenger_stats['avg_tickets_per_passenger']
    }
    
    conn.close()
    return jsonify({
        'aircraft_utilization': aircraft_utilization,
        'passenger_retention': passenger_retention
    })

@app.route('/api/advanced-metrics')
def advanced_metrics():
    """Get advanced SQL metrics (percentiles, rankings)"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Price statistics by fare class
    cursor.execute("""
        SELECT 
            fare_conditions,
            MIN(amount) as min_price,
            MAX(amount) as max_price,
            ROUND(AVG(amount), 2) as avg_price,
            COUNT(*) as ticket_count
        FROM ticket_flights
        WHERE fare_conditions IS NOT NULL
        GROUP BY fare_conditions
        ORDER BY avg_price DESC
    """)
    price_percentiles = [dict(row) for row in cursor.fetchall()]
    
    # Top performing routes by revenue
    cursor.execute("""
        SELECT 
            f.departure_airport || ' → ' || f.arrival_airport as route,
            ROUND(SUM(tf.amount), 2) as revenue,
            COUNT(tf.ticket_no) as tickets,
            ROUND(SUM(tf.amount) / COUNT(tf.ticket_no), 2) as revenue_per_ticket
        FROM ticket_flights tf
        JOIN flights f ON tf.flight_id = f.flight_id
        GROUP BY f.departure_airport, f.arrival_airport
        ORDER BY revenue DESC
        LIMIT 15
    """)
    top_routes = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    return jsonify({
        'price_percentiles': price_percentiles,
        'top_routes': top_routes
    })

if __name__ == '__main__':
    app.run(debug=True)