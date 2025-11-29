import sys
import os
from flask import Flask, render_template, url_for, redirect, request, jsonify
import sqlite3
import json
import time  # Added for performance timing
from datetime import datetime
from flask_pymongo import PyMongo
from bson import ObjectId

app = Flask(__name__)
app.config['MONGO_URI'] = 'mongodb://localhost:27017/travel_nosql'
mongo = PyMongo(app)

# Add the path to the parent directory to the sys.path list
sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))

# Database configuration - UPDATED PATH
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQLITE_DB = os.path.join(PROJECT_ROOT, 'travel.sqlite')

print(f"Looking for database at: {SQLITE_DB}")
print(f"Database exists: {os.path.exists(SQLITE_DB)}")

def get_db_connection():
    """Get SQLite database connection"""
    try:
        if not os.path.exists(SQLITE_DB):
            raise FileNotFoundError(f"Database file not found at: {SQLITE_DB}")
        
        # Set isolation_level=None for manual transaction control
        conn = sqlite3.connect(SQLITE_DB, isolation_level=None) 
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        raise

# === SQL PERFORMANCE HELPER (Updated to return Logs) ===
def execute_and_time(cursor, query, params=(), label="Query"):
    log_data = {"label": label, "type": "SQL", "plan": []}
    
    # 1. Explain Plan (Verify B+ Tree Usage)
    try:
        explain_query = f"EXPLAIN QUERY PLAN {query}"
        cursor.execute(explain_query, params)
        plan = cursor.fetchall()
        for row in plan:
            log_data["plan"].append(row['detail'])
    except Exception as e:
        log_data["plan"].append(f"Could not explain plan: {e}")

    # 2. Measure Execution Time
    start_time = time.perf_counter()
    cursor.execute(query, params)
    results = cursor.fetchall()
    duration_ms = (time.perf_counter() - start_time) * 1000
    
    log_data["duration"] = round(duration_ms, 4)
    
    # Return tuple: (Data, Log)
    return [dict(row) for row in results], log_data

# === NOSQL PERFORMANCE HELPER (Updated to return Logs) ===
def execute_nosql_and_time(collection, pipeline, label="NoSQL Query"):
    log_data = {"label": label, "type": "NoSQL", "plan": []}
    
    # 1. Verify Index Usage (Explain Plan)
    try:
        explanation = mongo.db.command(
            'aggregate', collection.name,
            pipeline=pipeline,
            explain=True
        )
        
        # FIX: Added default=str to handle Binary/ObjectId types
        plan_str = json.dumps(explanation, default=str)
        
        if "IXSCAN" in plan_str:
            log_data["plan"].append("✅ Used Index Scan (IXSCAN)")
        elif "COLLSCAN" in plan_str:
            log_data["plan"].append("⚠️ Used Collection Scan (COLLSCAN)")
        else:
            log_data["plan"].append("ℹ️ Complex Stage (See raw explain)")
            
    except Exception as e:
        # Improved error logging to see what actually failed
        log_data["plan"].append(f"Could not explain plan: {str(e)}")

    # 2. Measure Execution Time
    start_time = time.perf_counter()
    results = list(collection.aggregate(pipeline))
    duration_ms = (time.perf_counter() - start_time) * 1000
    
    log_data["duration"] = round(duration_ms, 4)
    
    return results, log_data

def extract_json_value(json_str):
    """Extract value from JSON string (for model, city, airport_name fields)"""
    if not json_str: return "Unknown"
    try:
        data = json.loads(json_str)
        if isinstance(data, dict):
            return data.get('en', list(data.values())[0] if data else 'Unknown')
        return str(data)
    except:
        return str(json_str)

# Initialize the database view when app starts
def init_db():
    try:
        print("Initializing database views and indexes...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("DROP VIEW IF EXISTS flight_routes;")
        cursor.execute("""
            CREATE VIEW flight_routes AS
            SELECT f.*, f.departure_airport || ' -> ' || f.arrival_airport AS route
            FROM flights f;
        """)

        # ==================== SQL INDEXES ====================
        print("Ensuring SQL B+ Tree Indexes...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_flights_perf ON flights (status, scheduled_arrival, actual_arrival, departure_airport, arrival_airport);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_flights_route_lookup ON flights (departure_airport, arrival_airport);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_seats_aircraft ON seats (aircraft_code);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_boarding_flight ON boarding_passes (flight_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ticketflights_revenue ON ticket_flights (flight_id, fare_conditions, amount);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_flights_aircraft_arrival ON flights (aircraft_code, arrival_airport);")
        conn.close() 

        # ==================== MONGODB INDEXES & VALIDATION ====================
        print("Ensuring MongoDB Indexes & Validation...")
        
        # 1. B-Tree Indexes
        mongo.db.flights.create_index([("status", 1), ("scheduled_departure", 1)])
        mongo.db.flights.create_index("flight_no")
        mongo.db.flights.create_index("departure.airport_code")
        mongo.db.flights.create_index("arrival.airport_code")
        mongo.db.flights.create_index("aircraft.code")
        mongo.db.bookings.create_index("tickets.ticket_no")
        
        # 2. Text Index for Full-Text Search
        mongo.db.flights.create_index([
            ("flight_no", "text"),
            ("departure.airport_code", "text"),
            ("arrival.airport_code", "text")
        ])
        
        # 3. Schema Validation
        flight_validator = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["flight_no", "status", "scheduled_departure"],
                "properties": {
                    "flight_no": {
                        "bsonType": "string",
                        "description": "must be a string and is required"
                    },
                    "status": {
                        "enum": ["Scheduled", "On Time", "Delayed", "Departed", "Arrived", "Cancelled"],
                        "description": "can only be one of the enum values"
                    },
                    "scheduled_departure": {
                        "bsonType": "string",
                        "description": "must be a date string"
                    }
                }
            }
        }
        try:
            mongo.db.create_collection("flights")
        except:
            pass 
            
        try:
            mongo.db.command("collMod", "flights", validator=flight_validator)
            print("[OK] Flights Schema Validator Applied")
        except Exception as e:
            print(f"Warning: Could not apply validator: {e}")
            
        print("[OK] All Indexes Initialized")

    except Exception as e:
        print(f"Error initializing database: {e}")
        raise

@app.route('/')
def index(): return render_template('index.html')

@app.route('/attributes')
def attributes(): return render_template('attributes.html')

# ==================== SQL ANALYTICS (With Timing Logs) ====================

@app.route('/api/flight-operations')
def flight_operations():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        q1 = """
            SELECT route, 
            ROUND(AVG((JULIANDAY(SUBSTR(actual_arrival, 1, 19)) - JULIANDAY(SUBSTR(scheduled_arrival, 1, 19))) * 24 * 60), 2) AS avg_delay_mins 
            FROM flight_routes 
            WHERE status = 'Arrived' 
            AND actual_arrival IS NOT NULL 
            AND scheduled_arrival IS NOT NULL 
            AND JULIANDAY(SUBSTR(actual_arrival, 1, 19)) > JULIANDAY(SUBSTR(scheduled_arrival, 1, 19)) 
            GROUP BY route 
            HAVING avg_delay_mins > 60  -- CHANGED: Filter for > 60 mins
            ORDER BY avg_delay_mins DESC 
            LIMIT 50; -- CHANGED: Increased limit to show more routes
        """
        least_punctual_routes, log1 = execute_and_time(cursor, q1, label="Least Punctual Routes")
        
        q2 = "SELECT COUNT(*) as total_flights, SUM(CASE WHEN status LIKE '%Delayed%' OR status = 'Delayed' THEN 1 ELSE 0 END) as delayed_flights, SUM(CASE WHEN status LIKE '%Cancel%' OR status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled_flights, SUM(CASE WHEN status = 'Arrived' OR status = 'On Time' THEN 1 ELSE 0 END) as ontime_flights FROM flights"
        overview_res, log2 = execute_and_time(cursor, q2, label="Overview Metrics")
        overview = overview_res[0] if overview_res else {}
        
        q3 = "SELECT ROUND(AVG((JULIANDAY(SUBSTR(actual_arrival, 1, 19)) - JULIANDAY(SUBSTR(scheduled_arrival, 1, 19))) * 24 * 60), 2) as avg_delay_minutes FROM flights WHERE actual_arrival IS NOT NULL AND scheduled_arrival IS NOT NULL AND status = 'Arrived' AND JULIANDAY(SUBSTR(actual_arrival, 1, 19)) > JULIANDAY(SUBSTR(scheduled_arrival, 1, 19))"
        delay_res, log3 = execute_and_time(cursor, q3, label="Avg Delay Calculation")
        overview['avg_delay_minutes'] = delay_res[0]['avg_delay_minutes'] if delay_res and delay_res[0]['avg_delay_minutes'] else 0
        
        conn.close()
        # Return data AND performance logs
        return jsonify({'least_punctual_routes': least_punctual_routes, 'overview': overview, '_perf': [log1, log2, log3]})
    except Exception as e: return jsonify({'error': str(e)}), 500

@app.route('/api/route-performance')
def route_performance():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = "SELECT fr.route, COUNT(fr.flight_id) AS flight_count FROM flight_routes AS fr GROUP BY fr.route ORDER BY flight_count DESC LIMIT 10;"
        busiest_routes, log1 = execute_and_time(cursor, query, label="Route Performance")
        conn.close()
        return jsonify({'busiest_routes': busiest_routes, '_perf': [log1]})
    except Exception as e: return jsonify({'error': str(e)}), 500

@app.route('/api/passenger-demand')
def passenger_demand():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        q1 = "WITH FlightCapacity AS (SELECT fr.flight_id, fr.route, COUNT(s.seat_no) AS total_seats FROM flight_routes AS fr JOIN seats AS s ON fr.aircraft_code = s.aircraft_code GROUP BY fr.flight_id, fr.route), FlightBookings AS (SELECT flight_id, COUNT(ticket_no) AS booked_seats FROM boarding_passes GROUP BY flight_id) SELECT fc.route, ROUND(AVG((fb.booked_seats * 100.0 / fc.total_seats)), 6) AS avg_occupancy_percent FROM FlightCapacity AS fc JOIN FlightBookings AS fb ON fc.flight_id = fb.flight_id WHERE fc.total_seats > 0 GROUP BY fc.route ORDER BY avg_occupancy_percent DESC LIMIT 10;"
        top_occupancy_routes, log1 = execute_and_time(cursor, q1, label="Passenger Occupancy")
        q2 = "WITH RouteBookings AS (SELECT fr.route, COUNT(tf.ticket_no) AS total_tickets_sold FROM flight_routes AS fr JOIN ticket_flights AS tf ON fr.flight_id = tf.flight_id GROUP BY fr.route), TotalTickets AS (SELECT CAST(COUNT(ticket_no) AS REAL) AS grand_total FROM ticket_flights) SELECT rb.route, rb.total_tickets_sold, ROUND((rb.total_tickets_sold * 100.0 / tt.grand_total), 6) AS market_share_percent FROM RouteBookings AS rb CROSS JOIN TotalTickets AS tt ORDER BY market_share_percent DESC LIMIT 10;"
        busiest_routes_market_share, log2 = execute_and_time(cursor, q2, label="Market Share High")
        q3 = "WITH RouteBookings AS (SELECT fr.route, COUNT(tf.ticket_no) AS total_tickets_sold FROM flight_routes AS fr JOIN ticket_flights AS tf ON fr.flight_id = tf.flight_id GROUP BY fr.route), TotalTickets AS (SELECT CAST(COUNT(ticket_no) AS REAL) AS grand_total FROM ticket_flights) SELECT rb.route, rb.total_tickets_sold, ROUND((rb.total_tickets_sold * 100.0 / tt.grand_total), 6) AS market_share_percent FROM RouteBookings AS rb CROSS JOIN TotalTickets AS tt WHERE rb.total_tickets_sold > 0 ORDER BY market_share_percent ASC LIMIT 10;"
        least_busy_routes, log3 = execute_and_time(cursor, q3, label="Market Share Low")
        conn.close()
        return jsonify({'top_occupancy_routes': top_occupancy_routes, 'busiest_routes_market_share': busiest_routes_market_share, 'least_busy_routes': least_busy_routes, '_perf': [log1, log2, log3]})
    except Exception as e: return jsonify({'error': str(e)}), 500

@app.route('/api/revenue-analysis')
def revenue_analysis():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        q1 = "SELECT tf.fare_conditions, SUM(tf.amount) AS total_revenue_by_class FROM ticket_flights AS tf GROUP BY tf.fare_conditions ORDER BY total_revenue_by_class DESC;"
        revenue_by_class, log1 = execute_and_time(cursor, q1, label="Revenue by Class")
        q2 = "SELECT fr.route, SUM(tf.amount) AS total_revenue FROM flight_routes AS fr JOIN ticket_flights AS tf ON fr.flight_id = tf.flight_id GROUP BY fr.route ORDER BY total_revenue DESC LIMIT 3;"
        top_revenue_routes, log2 = execute_and_time(cursor, q2, label="Top Revenue Routes")
        q3 = "SELECT fr.route, SUM(tf.amount) AS total_revenue FROM flight_routes AS fr JOIN ticket_flights AS tf ON fr.flight_id = tf.flight_id GROUP BY fr.route HAVING total_revenue > 0 ORDER BY total_revenue ASC LIMIT 3;"
        least_revenue_routes, log3 = execute_and_time(cursor, q3, label="Least Revenue Routes")
        q4 = "SELECT fr.route, tf.fare_conditions, SUM(tf.amount) AS total_revenue_by_class FROM flight_routes AS fr JOIN ticket_flights AS tf ON fr.flight_id = tf.flight_id GROUP BY fr.route, tf.fare_conditions ORDER BY total_revenue_by_class DESC LIMIT 20;"
        revenue_by_class_route, log4 = execute_and_time(cursor, q4, label="Rev by Route & Class")
        conn.close()
        return jsonify({'revenue_by_class': revenue_by_class, 'top_revenue_routes': top_revenue_routes, 'least_revenue_routes': least_revenue_routes, 'revenue_by_class_route': revenue_by_class_route, '_perf': [log1, log2, log3, log4]})
    except Exception as e: return jsonify({'error': str(e)}), 500

@app.route('/api/resource-planning')
def resource_planning():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        q1 = "SELECT fr.route, fr.aircraft_code, ad.model AS aircraft_model, COUNT(fr.flight_id) AS total_flights_on_route FROM flight_routes AS fr JOIN aircrafts_data AS ad ON fr.aircraft_code = ad.aircraft_code GROUP BY fr.route, fr.aircraft_code, ad.model ORDER BY total_flights_on_route DESC LIMIT 100;"
        aircraft_by_route_raw, log1 = execute_and_time(cursor, q1, label="Aircraft by Route")
        aircraft_by_route = []
        for row in aircraft_by_route_raw:
            d = row
            d['aircraft_model'] = extract_json_value(row['aircraft_model'])
            aircraft_by_route.append(d)
        
        q2 = "SELECT f.arrival_airport AS airport_code, ad.city AS destination_city, COUNT(f.flight_id) AS total_arrivals FROM flights AS f JOIN airports_data AS ad ON f.arrival_airport = ad.airport_code GROUP BY f.arrival_airport, destination_city ORDER BY total_arrivals DESC LIMIT 3;"
        destinations_raw, log2 = execute_and_time(cursor, q2, label="Top Destinations")
        top_destinations = []
        for row in destinations_raw:
            top_destinations.append({'airport_code': row['airport_code'], 'city': extract_json_value(row['destination_city']), 'total_arrivals': row['total_arrivals']})
            
        q3 = "SELECT f.aircraft_code, ad.model AS aircraft_model, SUM(ad.range) AS total_utilization_proxy_miles FROM flights AS f JOIN aircrafts_data AS ad ON f.aircraft_code = ad.aircraft_code WHERE f.status = 'Arrived' GROUP BY f.aircraft_code, aircraft_model ORDER BY total_utilization_proxy_miles DESC LIMIT 10;"
        aircraft_utilization_raw, log3 = execute_and_time(cursor, q3, label="Aircraft Utilization")
        aircraft_utilization = []
        for row in aircraft_utilization_raw:
            aircraft_utilization.append({'aircraft_code': row['aircraft_code'], 'aircraft_model': extract_json_value(row['aircraft_model']), 'total_mileage': row['total_utilization_proxy_miles']})
            
        q4 = "SELECT DISTINCT f.aircraft_code, ad.model AS aircraft_model, COUNT(f.flight_id) as flight_count FROM flights AS f JOIN aircrafts_data AS ad ON f.aircraft_code = ad.aircraft_code GROUP BY f.aircraft_code, ad.model ORDER BY flight_count DESC;"
        aircraft_list_raw, log4 = execute_and_time(cursor, q4, label="Aircraft List")
        aircraft_list = []
        for row in aircraft_list_raw:
            aircraft_list.append({'aircraft_code': row['aircraft_code'], 'aircraft_model': extract_json_value(row['aircraft_model']), 'flight_count': row['flight_count']})
            
        q5 = """SELECT ad.model, ROUND(AVG(CASE WHEN f.status = 'Arrived' AND f.actual_arrival IS NOT NULL THEN (JULIANDAY(SUBSTR(f.actual_arrival, 1, 19)) - JULIANDAY(SUBSTR(f.scheduled_arrival, 1, 19))) * 24 * 60 ELSE NULL END), 2) as avg_delay_minutes, SUM(CASE WHEN f.status = 'Cancelled' THEN 1 ELSE 0 END) as total_cancellations FROM flights f JOIN aircrafts_data ad ON f.aircraft_code = ad.aircraft_code
            GROUP BY ad.model ORDER BY total_cancellations DESC;
        """
        cancellation_stats, log5 = execute_and_time(cursor, q5, label="Cancellation Analysis")
        return jsonify({'aircraft_by_route': aircraft_by_route, 'top_destinations': top_destinations, 'aircraft_utilization': aircraft_utilization, 'aircraft_list': aircraft_list, 'cancellation_stats': cancellation_stats, '_perf': [log1, log2, log3, log4, log5]})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/aircraft-routes/<aircraft_code>')
def get_aircraft_routes(aircraft_code):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT fr.flight_id, fr.route, SUBSTR(fr.scheduled_departure, 1, 16) AS scheduled_departure_time, fr.status FROM flight_routes AS fr WHERE fr.aircraft_code = ? ORDER BY fr.scheduled_departure ASC LIMIT 50;", (aircraft_code,))
        routes = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify({'routes': routes})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== CRUD MANAGEMENT PAGES ====================
@app.route('/add_booking', methods=['GET', 'POST'])
def add_booking(): return render_template('add_booking.html', success=False)

@app.route('/crudManager')
def manage(): return render_template('crudManager.html')

# ==================== SQL CRUD (Transaction Managed) ====================
# (Keeping these exactly as they were)
def validate_column(col, allowed):
    return col if col in allowed else allowed[0]

@app.route('/api/flights', methods=['GET'])
def get_flights():
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        search = request.args.get('search', '', type=str)
        column = request.args.get('column', 'flight_no', type=str)
        
        allowed_cols = ['flight_id', 'flight_no', 'route', 'scheduled_departure', 'status', 'aircraft_code']
        safe_col = validate_column(column, allowed_cols)

        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Base Queries
        count_sql = "SELECT COUNT(*) FROM flights"
        data_sql = "SELECT * FROM flights"
        params = []

        if search:
            if safe_col == 'route':
                # Special logic for Route: Search Departure OR Arrival
                where_clause = "WHERE departure_airport LIKE ? OR arrival_airport LIKE ?"
                params = [f'%{search}%', f'%{search}%']
            else:
                where_clause = f"WHERE {safe_col} LIKE ?"
                params = [f'%{search}%']
            
            count_sql += " " + where_clause
            data_sql += " " + where_clause
        
        # Execute Count
        cursor.execute(count_sql, params)
        total = cursor.fetchone()[0]

        # Execute Data Query
        offset = (page - 1) * per_page
        data_sql += " ORDER BY scheduled_departure DESC LIMIT ? OFFSET ?"
        params.extend([per_page, offset])
        
        cursor.execute(data_sql, params)
        flights = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify({'flights': flights, 'total': total, 'page': page, 'per_page': per_page, 'total_pages': (total + per_page - 1) // per_page})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/flights/<int:flight_id>', methods=['GET'])
def get_flight(flight_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM flights WHERE flight_id = ?", (flight_id,))
        flight = cursor.fetchone()
        conn.close()
        if flight: return jsonify(dict(flight))
        return jsonify({'error': 'Flight not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/flights', methods=['POST'])
def create_flight():
    conn = None
    try:
        data = request.get_json()
        required_fields = ['flight_no', 'scheduled_departure', 'scheduled_arrival', 'departure_airport', 'arrival_airport', 'aircraft_code']
        for field in required_fields:
            if field not in data: return jsonify({'error': f'Missing required field: {field}'}), 400
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute("INSERT INTO flights (flight_no, scheduled_departure, scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code, actual_departure, actual_arrival) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (data['flight_no'], data['scheduled_departure'], data['scheduled_arrival'], data['departure_airport'], data['arrival_airport'], data.get('status', 'Scheduled'), data['aircraft_code'], data.get('actual_departure'), data.get('actual_arrival')))
        flight_id = cursor.lastrowid
        conn.commit()
        return jsonify({'message': 'Flight created successfully', 'flight_id': flight_id}), 201
    except sqlite3.IntegrityError as e:
        if conn: conn.rollback()
        return jsonify({'error': f'Database integrity error: {str(e)}'}), 400
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/flights/<int:flight_id>', methods=['PUT'])
def update_flight(flight_id):
    conn = None
    try:
        data = request.get_json()
        conn = get_db_connection()
        cursor = conn.cursor()
        update_fields = []
        values = []
        allowed_fields = ['flight_no', 'scheduled_departure', 'scheduled_arrival', 'departure_airport', 'arrival_airport', 'status', 'aircraft_code', 'actual_departure', 'actual_arrival']
        for field in allowed_fields:
            if field in data:
                update_fields.append(f"{field} = ?")
                values.append(data[field])
        if not update_fields: return jsonify({'error': 'No fields to update'}), 400
        values.append(flight_id)
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute(f"UPDATE flights SET {', '.join(update_fields)} WHERE flight_id = ?", values)
        if cursor.rowcount == 0:
            conn.rollback()
            return jsonify({'error': 'Flight not found'}), 404
        conn.commit()
        return jsonify({'message': 'Flight updated successfully'})
    except sqlite3.IntegrityError as e:
        if conn: conn.rollback()
        return jsonify({'error': f'Database integrity error: {str(e)}'}), 400
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/flights/<int:flight_id>', methods=['DELETE'])
def delete_flight(flight_id):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute("SELECT flight_id FROM flights WHERE flight_id = ?", (flight_id,))
        if not cursor.fetchone():
            conn.rollback()
            return jsonify({'error': 'Flight not found'}), 404
        cursor.execute("DELETE FROM ticket_flights WHERE flight_id = ?", (flight_id,))
        cursor.execute("DELETE FROM boarding_passes WHERE flight_id = ?", (flight_id,))
        cursor.execute("DELETE FROM flights WHERE flight_id = ?", (flight_id,))
        conn.commit()
        return jsonify({'message': 'Flight deleted successfully'})
    except sqlite3.IntegrityError as e:
        if conn: conn.rollback()
        return jsonify({'error': f'Cannot delete flight: {str(e)}'}), 400
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/bookings', methods=['GET'])
def get_bookings():
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        search = request.args.get('search', '', type=str)
        column = request.args.get('column', 'ticket_no', type=str)

        # UPDATED: Only columns that exist in your relational.py schema
        col_map = {
            'ticket_no': 't.ticket_no',
            'book_ref': 't.book_ref',
            'passenger_id': 't.passenger_id'
        }
        safe_col = col_map.get(column, 't.ticket_no')

        conn = get_db_connection()
        cursor = conn.cursor()
        
        base_query = "SELECT t.ticket_no, t.book_ref, t.passenger_id, b.book_date FROM tickets t JOIN bookings b ON t.book_ref = b.book_ref"
        
        if search:
            cursor.execute(f"SELECT COUNT(*) FROM tickets t JOIN bookings b ON t.book_ref = b.book_ref WHERE {safe_col} LIKE ?", (f'%{search}%',))
        else:
            cursor.execute("SELECT COUNT(*) FROM tickets")
        total = cursor.fetchone()[0]
        
        offset = (page - 1) * per_page
        if search:
            sql = f"{base_query} WHERE {safe_col} LIKE ? ORDER BY b.book_date DESC LIMIT ? OFFSET ?"
            cursor.execute(sql, (f'%{search}%', per_page, offset))
        else:
            sql = f"{base_query} ORDER BY b.book_date DESC LIMIT ? OFFSET ?"
            cursor.execute(sql, (per_page, offset))
            
        bookings = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify({'bookings': bookings, 'total': total, 'page': page, 'per_page': per_page, 'total_pages': (total + per_page - 1) // per_page})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/bookings/<ticket_no>', methods=['GET'])
def get_booking(ticket_no):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # UPDATED: Selects strictly from schema
        cursor.execute("SELECT ticket_no, book_ref, passenger_id FROM tickets WHERE ticket_no = ?", (ticket_no,))
        booking = cursor.fetchone()
        conn.close()
        if booking: return jsonify(dict(booking))
        return jsonify({'error': 'Booking not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/bookings', methods=['POST'])
def create_booking():
    conn = None
    try:
        data = request.get_json()
        required_fields = ['ticket_no', 'book_ref', 'passenger_id']
        for field in required_fields:
            if field not in data: return jsonify({'error': f'Missing required field: {field}'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("BEGIN TRANSACTION")
        
        # Ensure Booking Reference exists first (FK Constraint)
        cursor.execute("SELECT 1 FROM bookings WHERE book_ref = ?", (data['book_ref'],))
        if not cursor.fetchone():
             # If booking ref doesn't exist, create it (simple logic for this demo)
             cursor.execute("INSERT INTO bookings (book_ref, book_date, total_amount) VALUES (?, datetime('now'), 0)", (data['book_ref'],))

        cursor.execute("INSERT INTO tickets (ticket_no, book_ref, passenger_id) VALUES (?, ?, ?)", 
                       (data['ticket_no'], data['book_ref'], data['passenger_id']))
        conn.commit()
        return jsonify({'message': 'Booking created successfully'}), 201
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/bookings/<ticket_no>', methods=['PUT'])
def update_booking(ticket_no):
    conn = None
    try:
        data = request.get_json()
        conn = get_db_connection()
        cursor = conn.cursor()
        update_fields = []
        values = []
        
        # UPDATED: Only allow updating passenger_id (Ticket No and Book Ref usually static)
        if 'passenger_id' in data:
            update_fields.append("passenger_id = ?")
            values.append(data['passenger_id'])
            
        if not update_fields: return jsonify({'error': 'No valid fields provided (only passenger_id can be updated)'}), 400
        
        values.append(ticket_no)
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute(f"UPDATE tickets SET {', '.join(update_fields)} WHERE ticket_no = ?", values)
        
        if cursor.rowcount == 0:
            conn.rollback()
            return jsonify({'error': 'Booking not found'}), 404
        conn.commit()
        return jsonify({'message': 'Booking updated successfully'})
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/bookings/<ticket_no>', methods=['DELETE'])
def delete_booking(ticket_no):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("BEGIN TRANSACTION")
        # Cascade delete logic
        cursor.execute("DELETE FROM ticket_flights WHERE ticket_no = ?", (ticket_no,))
        cursor.execute("DELETE FROM boarding_passes WHERE ticket_no = ?", (ticket_no,))
        cursor.execute("DELETE FROM tickets WHERE ticket_no = ?", (ticket_no,))
        if cursor.rowcount == 0:
            conn.rollback()
            return jsonify({'error': 'Booking not found'}), 404
        conn.commit()
        return jsonify({'message': 'Booking deleted successfully'})
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/aircraft', methods=['GET'])
def get_aircraft():
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        search = request.args.get('search', '', type=str)
        column = request.args.get('column', 'aircraft_code', type=str)

        # Validate Column
        allowed = ['aircraft_code', 'model', 'range']
        safe_col = validate_column(column, allowed)

        conn = get_db_connection()
        cursor = conn.cursor()
        
        if search:
            cursor.execute(f"SELECT COUNT(*) FROM aircrafts_data WHERE {safe_col} LIKE ?", (f'%{search}%',))
        else:
            cursor.execute("SELECT COUNT(*) FROM aircrafts_data")
        total = cursor.fetchone()[0]
        
        offset = (page - 1) * per_page
        if search:
            cursor.execute(f"SELECT * FROM aircrafts_data WHERE {safe_col} LIKE ? LIMIT ? OFFSET ?", (f'%{search}%', per_page, offset))
        else:
            cursor.execute("SELECT * FROM aircrafts_data LIMIT ? OFFSET ?", (per_page, offset))
            
        aircraft_raw = cursor.fetchall()
        aircraft = []
        for row in aircraft_raw:
            d = dict(row)
            d['model'] = extract_json_value(d.get('model'))
            aircraft.append(d)
        conn.close()
        return jsonify({'aircraft': aircraft, 'total': total, 'page': page, 'per_page': per_page, 'total_pages': (total + per_page - 1) // per_page})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/aircraft/<aircraft_code>', methods=['GET'])
def get_single_aircraft(aircraft_code):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM aircrafts_data WHERE aircraft_code = ?", (aircraft_code,))
        ac = cursor.fetchone()
        conn.close()
        if ac:
            d = dict(ac)
            d['model'] = extract_json_value(d.get('model'))
            return jsonify(d)
        return jsonify({'error': 'Aircraft not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/aircraft', methods=['POST'])
def create_aircraft():
    conn = None
    try:
        data = request.get_json()
        required_fields = ['aircraft_code', 'model', 'range']
        for field in required_fields:
            if field not in data: return jsonify({'error': f'Missing required field: {field}'}), 400
        conn = get_db_connection()
        cursor = conn.cursor()
        model = data['model']
        if isinstance(model, str): model = json.dumps({"en": model})
        elif isinstance(model, dict): model = json.dumps(model)
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute("INSERT INTO aircrafts_data (aircraft_code, model, range) VALUES (?, ?, ?)", (data['aircraft_code'], model, data['range']))
        conn.commit()
        return jsonify({'message': 'Aircraft created successfully'}), 201
    except sqlite3.IntegrityError as e:
        if conn: conn.rollback()
        return jsonify({'error': f'Aircraft code already exists or integrity error: {str(e)}'}), 400
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/aircraft/<aircraft_code>', methods=['PUT'])
def update_aircraft(aircraft_code):
    conn = None
    try:
        data = request.get_json()
        conn = get_db_connection()
        cursor = conn.cursor()
        update_fields = []
        values = []
        if 'model' in data:
            model = data['model']
            if isinstance(model, str): model = json.dumps({"en": model})
            elif isinstance(model, dict): model = json.dumps(model)
            update_fields.append("model = ?")
            values.append(model)
        if 'range' in data:
            update_fields.append("range = ?")
            values.append(data['range'])
        if not update_fields: return jsonify({'error': 'No fields to update'}), 400
        values.append(aircraft_code)
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute(f"UPDATE aircrafts_data SET {', '.join(update_fields)} WHERE aircraft_code = ?", values)
        if cursor.rowcount == 0:
            conn.rollback()
            return jsonify({'error': 'Aircraft not found'}), 404
        conn.commit()
        return jsonify({'message': 'Aircraft updated successfully'})
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/aircraft/<aircraft_code>', methods=['DELETE'])
def delete_aircraft(aircraft_code):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute("SELECT COUNT(*) FROM flights WHERE aircraft_code = ?", (aircraft_code,))
        if cursor.fetchone()[0] > 0:
            conn.rollback()
            return jsonify({'error': 'Cannot delete assigned aircraft'}), 400
        cursor.execute("DELETE FROM seats WHERE aircraft_code = ?", (aircraft_code,))
        cursor.execute("DELETE FROM aircrafts_data WHERE aircraft_code = ?", (aircraft_code,))
        if cursor.rowcount == 0:
            conn.rollback()
            return jsonify({'error': 'Aircraft not found'}), 404
        conn.commit()
        return jsonify({'message': 'Aircraft deleted successfully'})
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()

# === AIRPORTS CRUD (SQL) ===
@app.route('/api/airports', methods=['GET'])
def get_airports():
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        search = request.args.get('search', '', type=str)
        column = request.args.get('column', 'airport_code', type=str)

        allowed = ['airport_code', 'airport_name', 'city', 'timezone']
        safe_col = validate_column(column, allowed)

        conn = get_db_connection()
        cursor = conn.cursor()
        
        if search:
            cursor.execute(f"SELECT COUNT(*) FROM airports_data WHERE {safe_col} LIKE ?", (f'%{search}%',))
        else:
            cursor.execute("SELECT COUNT(*) FROM airports_data")
        total = cursor.fetchone()[0]
        
        offset = (page - 1) * per_page
        if search:
            cursor.execute(f"SELECT * FROM airports_data WHERE {safe_col} LIKE ? LIMIT ? OFFSET ?", (f'%{search}%', per_page, offset))
        else:
            cursor.execute("SELECT * FROM airports_data LIMIT ? OFFSET ?", (per_page, offset))
        
        raw_airports = cursor.fetchall()
        airports = []
        for row in raw_airports:
            d = dict(row)
            d['airport_name'] = extract_json_value(d['airport_name'])
            d['city'] = extract_json_value(d['city'])
            airports.append(d)
        conn.close()
        return jsonify({'airports': airports, 'total': total, 'page': page, 'per_page': per_page, 'total_pages': (total + per_page - 1) // per_page})
    except Exception as e: return jsonify({'error': str(e)}), 500

@app.route('/api/airports/<airport_code>', methods=['GET'])
def get_single_airport(airport_code):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM airports_data WHERE airport_code = ?", (airport_code,))
        ap = cursor.fetchone()
        conn.close()
        if ap:
            d = dict(ap)
            d['airport_name'] = extract_json_value(d['airport_name'])
            d['city'] = extract_json_value(d['city'])
            return jsonify(d)
        return jsonify({'error': 'Airport not found'}), 404
    except Exception as e: return jsonify({'error': str(e)}), 500

@app.route('/api/airports', methods=['POST'])
def create_airport():
    conn = None
    try:
        data = request.get_json()
        conn = get_db_connection()
        cursor = conn.cursor()
        name = json.dumps({"en": data['airport_name']})
        city = json.dumps({"en": data['city']})
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute("INSERT INTO airports_data (airport_code, airport_name, city, coordinates, timezone) VALUES (?, ?, ?, ?, ?)", 
                       (data['airport_code'], name, city, data.get('coordinates'), data.get('timezone')))
        conn.commit()
        return jsonify({'message': 'Airport created'}), 201
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/airports/<airport_code>', methods=['PUT'])
def update_airport(airport_code):
    conn = None
    try:
        data = request.get_json()
        conn = get_db_connection()
        cursor = conn.cursor()
        update_fields = []
        values = []
        if 'airport_name' in data:
            update_fields.append("airport_name = ?")
            values.append(json.dumps({"en": data['airport_name']}))
        if 'city' in data:
            update_fields.append("city = ?")
            values.append(json.dumps({"en": data['city']}))
        if 'timezone' in data:
            update_fields.append("timezone = ?")
            values.append(data['timezone'])
        if 'coordinates' in data:
            update_fields.append("coordinates = ?")
            values.append(data['coordinates'])
        if not update_fields: return jsonify({'error': 'No fields'}), 400
        values.append(airport_code)
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute(f"UPDATE airports_data SET {', '.join(update_fields)} WHERE airport_code = ?", values)
        conn.commit()
        return jsonify({'message': 'Airport updated'})
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()

@app.route('/api/airports/<airport_code>', methods=['DELETE'])
def delete_airport(airport_code):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute("SELECT COUNT(*) FROM flights WHERE departure_airport = ? OR arrival_airport = ?", (airport_code, airport_code))
        if cursor.fetchone()[0] > 0:
            conn.rollback()
            return jsonify({'error': 'Cannot delete airport with assigned flights'}), 400
        cursor.execute("DELETE FROM airports_data WHERE airport_code = ?", (airport_code,))
        conn.commit()
        return jsonify({'message': 'Airport deleted'})
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()

# =========================================================
# NOSQL (MONGODB) API ENDPOINTS
# =========================================================

# --- NoSQL Analytics Endpoints (With Timing & Explain) ---

@app.route('/api/nosql/flight-operations')
def nosql_flight_operations():
    # 1. Overview Metrics (Counts)
    pipeline_overview = [
        {"$group": {
            "_id": None,
            "total_flights": {"$sum": 1},
            "delayed": {"$sum": {"$cond": [{"$regexMatch": {"input": "$status", "regex": "Delayed"}}, 1, 0]}},
            "cancelled": {"$sum": {"$cond": [{"$regexMatch": {"input": "$status", "regex": "Cancel"}}, 1, 0]}},
            "ontime": {"$sum": {"$cond": [{"$in": ["$status", ["Arrived", "On Time"]]}, 1, 0]}}
        }}
    ]
    overview_stats, log1 = execute_nosql_and_time(mongo.db.flights, pipeline_overview, label="Overview Metrics")
    overview = overview_stats[0] if overview_stats else {"total_flights": 0}

    # 2. Avg Delay Calculation (Time Diff)
    pipeline_delay = [
        # Filter for Arrived flights with valid timestamps
        {"$match": {
            "status": "Arrived",
            "actual_arrival": {"$exists": True, "$ne": None},
            "scheduled_arrival": {"$exists": True, "$ne": None}
        }},
        # Calculate difference in milliseconds
        {"$project": {
            "delay_ms": {
                "$subtract": [
                    {"$toDate": "$actual_arrival"},
                    {"$toDate": "$scheduled_arrival"}
                ]
            }
        }},
        # Only consider flights that were actually late (> 0)
        {"$match": {"delay_ms": {"$gt": 0}}},
        # Calculate Average
        {"$group": {
            "_id": None,
            "avg_delay_ms": {"$avg": "$delay_ms"}
        }}
    ]
    
    delay_stats, log_delay = execute_nosql_and_time(mongo.db.flights, pipeline_delay, label="Avg Delay Calc")
    
    # Process the delay result (convert ms to minutes)
    avg_delay_val = 0
    if delay_stats and len(delay_stats) > 0:
        avg_ms = delay_stats[0].get('avg_delay_ms', 0)
        avg_delay_val = round(avg_ms / 60000, 2) # 60000 ms = 1 minute

    overview_data = {
        "total_flights": overview.get('total_flights', 0),
        "delayed_flights": overview.get('delayed', 0),
        "cancelled_flights": overview.get('cancelled', 0),
        "ontime_flights": overview.get('ontime', 0),
        "avg_delay_minutes": avg_delay_val # Assigned calculated value
    }

    # 3. Least Punctual Routes (Avg > 60 mins)
    pipeline_punctual = [
        {"$match": {
            "status": "Arrived",
            "actual_arrival": {"$exists": True, "$ne": None},
            "scheduled_arrival": {"$exists": True, "$ne": None}
        }},
        {"$project": {
            "route": {"$concat": ["$departure.airport_code", " -> ", "$arrival.airport_code"]},
            "delay_minutes": {
                "$divide": [
                    {"$subtract": [{"$toDate": "$actual_arrival"}, {"$toDate": "$scheduled_arrival"}]},
                    60000
                ]
            }
        }},
        # Filter for positive delays first
        {"$match": {"delay_minutes": {"$gt": 0}}},
        
        # Calculate Average per Route
        {"$group": {"_id": "$route", "avg_delay": {"$avg": "$delay_minutes"}}},
        
        # CHANGED: Only show routes where Avg Delay is > 60 Minutes
        {"$match": {"avg_delay": {"$gt": 60}}},
        
        {"$sort": {"avg_delay": -1}},
        # CHANGED: Increased limit to 50 to show more than just top 5
        {"$limit": 50}
    ]
    punctual_data_raw, log2 = execute_nosql_and_time(mongo.db.flights, pipeline_punctual, label="Least Punctual")
    punctual_data = [{"route": doc["_id"], "avg_delay_mins": round(doc["avg_delay"], 2)} for doc in punctual_data_raw]

    return jsonify({'least_punctual_routes': punctual_data, 'overview': overview_data, '_perf': [log1, log_delay, log2]})

@app.route('/api/nosql/route-performance')
def nosql_route_performance():
    pipeline = [
        {"$group": {
            "_id": {"$concat": ["$departure.airport_code", " -> ", "$arrival.airport_code"]},
            "flight_count": {"$sum": 1}
        }},
        {"$sort": {"flight_count": -1}},
        {"$limit": 10}
    ]
    busiest_raw, log1 = execute_nosql_and_time(mongo.db.flights, pipeline, label="Route Performance")
    busiest = [{"route": doc["_id"], "flight_count": doc["flight_count"]} for doc in busiest_raw]
    return jsonify({'busiest_routes': busiest, '_perf': [log1]})

@app.route('/api/nosql/resource-planning')
def nosql_resource_planning():
    # ---------------------------------------------------------
    # 1. Aircraft by Route (Chart 8 - Stacked Bar)
    # ---------------------------------------------------------
    pipeline_aircraft_route = [
        {"$group": {
            "_id": {
                "route": {"$concat": ["$departure.airport_code", " -> ", "$arrival.airport_code"]},
                "code": "$aircraft.code",
                "model": "$aircraft.model"
            },
            "count": {"$sum": 1}
        }},
        {"$project": {
            "route": "$_id.route",
            "aircraft_code": "$_id.code",
            "aircraft_model": "$_id.model",
            "total_flights_on_route": "$count",
            "_id": 0
        }},
        {"$sort": {"total_flights_on_route": -1}},
        {"$limit": 100} # Increased limit for stacked bar
    ]
    aircraft_by_route, log1 = execute_nosql_and_time(mongo.db.flights, pipeline_aircraft_route, label="Aircraft by Route")

    # ---------------------------------------------------------
    # 2. Top Destinations (Chart 9)
    # ---------------------------------------------------------
    pipeline_destinations = [
        {"$group": {
            "_id": {"code": "$arrival.airport_code", "city": "$arrival.city"},
            "count": {"$sum": 1}
        }},
        {"$project": {
            "airport_code": "$_id.code",
            "city": "$_id.city",
            "total_arrivals": "$count",
            "_id": 0
        }},
        {"$sort": {"total_arrivals": -1}},
        {"$limit": 3}
    ]
    top_destinations, log2 = execute_nosql_and_time(mongo.db.flights, pipeline_destinations, label="Top Destinations")

    # ---------------------------------------------------------
    # 3. Aircraft Utilization (Chart 10)
    # ---------------------------------------------------------
    pipeline_utilization = [
        {"$match": {"status": "Arrived"}},
        {"$group": {
            "_id": "$aircraft.code",
            "count": {"$sum": 1},
            "model": {"$first": "$aircraft.model"}
        }},
        {"$lookup": {
            "from": "aircrafts",
            "localField": "_id",
            "foreignField": "_id",
            "as": "ac_info"
        }},
        {"$unwind": "$ac_info"},
        {"$project": {
            "aircraft_code": "$_id",
            "aircraft_model": "$model",
            "total_mileage": {"$multiply": ["$count", "$ac_info.range"]},
            "_id": 0
        }},
        {"$sort": {"total_mileage": -1}},
        {"$limit": 10}
    ]
    aircraft_utilization, log3 = execute_nosql_and_time(mongo.db.flights, pipeline_utilization, label="Aircraft Utilization")

    # ---------------------------------------------------------
    # 4. Aircraft List (For Filter Dropdowns if needed)
    # ---------------------------------------------------------
    pipeline_list = [
        {"$group": {"_id": {"code": "$aircraft.code", "model": "$aircraft.model"}, "count": {"$sum": 1}}},
        {"$project": {"aircraft_code": "$_id.code", "aircraft_model": "$_id.model", "flight_count": "$count", "_id": 0}},
        {"$sort": {"flight_count": -1}}
    ]
    aircraft_list, log4 = execute_nosql_and_time(mongo.db.flights, pipeline_list, label="Aircraft List")

    # ---------------------------------------------------------
    # 5. Cancellation & Delay Analysis (Chart 11 - NEW)
    # ---------------------------------------------------------
    pipeline_cancel = [
        {"$group": {
            "_id": "$aircraft.code",
            "model": {"$first": "$aircraft.model"},
            # Logic: If Arrived, calculate time diff. If Cancelled, ignore for delay.
            "total_delay_ms": {
                "$sum": {
                    "$cond": [
                        {"$and": [{"$eq": ["$status", "Arrived"]}, {"$ne": ["$actual_arrival", None]}]},
                        {"$subtract": [{"$toDate": "$actual_arrival"}, {"$toDate": "$scheduled_arrival"}]},
                        0
                    ]
                }
            },
            "arrived_count": {
                "$sum": {"$cond": [{"$eq": ["$status", "Arrived"]}, 1, 0]}
            },
            "cancelled_count": {
                "$sum": {"$cond": [{"$eq": ["$status", "Cancelled"]}, 1, 0]}
            }
        }},
        {"$project": {
            "model": 1,
            "total_cancellations": "$cancelled_count",
            # Avoid division by zero
            "avg_delay_minutes": {
                "$cond": [
                    {"$gt": ["$arrived_count", 0]},
                    {"$round": [{"$divide": [{"$divide": ["$total_delay_ms", "$arrived_count"]}, 60000]}, 2]},
                    0
                ]
            },
            "_id": 0
        }},
        {"$sort": {"total_cancellations": -1}}
    ]
    cancellation_stats, log5 = execute_nosql_and_time(mongo.db.flights, pipeline_cancel, label="Cancellation Analysis")

    return jsonify({
        'aircraft_by_route': aircraft_by_route,
        'top_destinations': top_destinations,
        'aircraft_utilization': aircraft_utilization,
        'aircraft_list': aircraft_list,
        'cancellation_stats': cancellation_stats,
        '_perf': [log1, log2, log3, log4, log5]
    })

@app.route('/api/nosql/passenger-demand')
def nosql_passenger_demand():
    # ==================================================================
    # SECTION A: OCCUPANCY RATE (Based on BOARDING PASSES / Flown)
    # ==================================================================
    
    # 1. Get Capacity per Aircraft
    ac_seats = {}
    ac_cursor = mongo.db.aircrafts.find({}, {"seats": 1})
    for ac in ac_cursor:
        seats = ac.get('seats', [])
        ac_seats[ac['_id']] = len(seats)

    # 2. Get Booked Count (FILTER: MUST HAVE BOARDING PASS)
    pipeline_flown_counts = [
        {"$unwind": "$tickets"},
        {"$unwind": "$tickets.flight_legs"},
        # CRITICAL: Only count if they actually boarded (Occupancy logic)
        {"$match": {"tickets.flight_legs.boarding_pass": {"$exists": True}}}, 
        {"$group": {
            "_id": "$tickets.flight_legs.flight_id", 
            "count": {"$sum": 1}
        }}
    ]
    flown_res, log_occ = execute_nosql_and_time(mongo.db.bookings, pipeline_flown_counts, "Occupancy: Count Flown")
    
    flown_map = {}
    for b in flown_res:
        flown_map[b['_id']] = b['count']

    # 3. Calculate Occupancy % per Route
    flights_cursor = mongo.db.flights.find({}, {
        "aircraft.code": 1, 
        "departure.airport_code": 1, 
        "arrival.airport_code": 1
    })

    route_stats = {} 
    for f in flights_cursor:
        flight_id = f['_id'] 
        ac_code = f.get('aircraft', {}).get('code')
        dep = f.get('departure', {}).get('airport_code')
        arr = f.get('arrival', {}).get('airport_code')
        
        if dep and arr and ac_code:
            route = f"{dep} -> {arr}"
            capacity = ac_seats.get(ac_code, 0)
            # Use FLOWN count here
            flown = flown_map.get(flight_id, 0)
            
            if capacity > 0 and flown > 0:
                occupancy = (flown / capacity) * 100
                if route not in route_stats: route_stats[route] = []
                route_stats[route].append(min(occupancy, 100.0))

    final_occupancy = []
    for route, occ_list in route_stats.items():
        avg_occ = sum(occ_list) / len(occ_list)
        final_occupancy.append({"route": route, "avg_occupancy_percent": round(avg_occ, 2)})

    final_occupancy.sort(key=lambda x: x['avg_occupancy_percent'], reverse=True)
    top_10_occupancy = final_occupancy[:10]

    # ==================================================================
    # SECTION B: MARKET SHARE (Based on TICKETS SOLD / Sales)
    # ==================================================================
    
    # 1. Calculate Grand Total of TICKETS SOLD (Ignore Boarding Passes)
    ticket_count_pipeline = [
        {"$unwind": "$tickets"},
        {"$unwind": "$tickets.flight_legs"},
        # CRITICAL: No match stage here! We count ALL sold tickets.
        {"$count": "total"}
    ]
    t_res = list(mongo.db.bookings.aggregate(ticket_count_pipeline))
    grand_total_tickets = t_res[0]['total'] if t_res else 1

    # 2. Busiest Routes Pipeline
    pipeline_market_share = [
        {"$unwind": "$tickets"},
        {"$unwind": "$tickets.flight_legs"},
        # CRITICAL: No match stage here either!
        {"$group": {
            "_id": "$tickets.flight_legs.route",
            "count": {"$sum": 1}
        }},
        {"$project": {
            "route": "$_id",
            "total_tickets_sold": "$count",
            "market_share_percent": {"$round": [{"$multiply": [{"$divide": ["$count", grand_total_tickets]}, 100]}, 6]}, # Using 6 precision
            "_id": 0
        }},
        {"$sort": {"market_share_percent": -1}},
        {"$limit": 10}
    ]
    busiest_routes, log1 = execute_nosql_and_time(mongo.db.bookings, pipeline_market_share, label="Market Share High")

    # 3. Least Busy Routes Pipeline
    pipeline_least_busy = [
        {"$unwind": "$tickets"},
        {"$unwind": "$tickets.flight_legs"},
        {"$group": {
            "_id": "$tickets.flight_legs.route",
            "count": {"$sum": 1}
        }},
        {"$project": {
            "route": "$_id",
            "market_share_percent": {"$round": [{"$multiply": [{"$divide": ["$count", grand_total_tickets]}, 100]}, 6]}, # Using 6 precision
            "_id": 0
        }},
        # Sort by percent ASC, then route name to ensure consistent "Bottom 10"
        {"$sort": {"market_share_percent": 1, "route": 1}},
        {"$limit": 10}
    ]
    least_busy, log2 = execute_nosql_and_time(mongo.db.bookings, pipeline_least_busy, label="Market Share Low")

    return jsonify({
        'top_occupancy_routes': top_10_occupancy, 
        'busiest_routes_market_share': busiest_routes, 
        'least_busy_routes': least_busy, 
        '_perf': [log_occ, log1, log2]
    })

@app.route('/api/nosql/revenue-analysis')
def nosql_revenue_analysis():
    # 1. Revenue by Fare Class
    pipeline_class = [
        {"$unwind": "$tickets"},
        {"$unwind": "$tickets.flight_legs"},
        {"$group": {
            "_id": "$tickets.flight_legs.fare_conditions",
            "total": {"$sum": "$tickets.flight_legs.amount"}
        }},
        {"$project": {
            "fare_conditions": "$_id",
            "total_revenue_by_class": "$total",
            "_id": 0
        }}
    ]
    rev_by_class, log1 = execute_nosql_and_time(mongo.db.bookings, pipeline_class, label="Revenue by Class")

    # 2. Top Revenue Routes
    pipeline_top_routes = [
        {"$unwind": "$tickets"},
        {"$unwind": "$tickets.flight_legs"},
        {"$group": {
            "_id": "$tickets.flight_legs.route",
            "total": {"$sum": "$tickets.flight_legs.amount"}
        }},
        {"$sort": {"total": -1}},
        {"$limit": 3},
        {"$project": {"route": "$_id", "total_revenue": "$total", "_id": 0}}
    ]
    top_rev, log2 = execute_nosql_and_time(mongo.db.bookings, pipeline_top_routes, label="Top Revenue Routes")

    # 3. Least Revenue Routes
    pipeline_low_routes = [
        {"$unwind": "$tickets"},
        {"$unwind": "$tickets.flight_legs"},
        {"$group": {
            "_id": "$tickets.flight_legs.route",
            "total": {"$sum": "$tickets.flight_legs.amount"}
        }},
        {"$sort": {"total": 1}},
        {"$limit": 3},
        {"$project": {"route": "$_id", "total_revenue": "$total", "_id": 0}}
    ]
    low_rev, log3 = execute_nosql_and_time(mongo.db.bookings, pipeline_low_routes, label="Least Revenue Routes")

    # 4. Revenue by Class & Route
    pipeline_complex = [
        {"$unwind": "$tickets"},
        {"$unwind": "$tickets.flight_legs"},
        {"$group": {
            "_id": {
                "route": "$tickets.flight_legs.route",
                "cond": "$tickets.flight_legs.fare_conditions"
            },
            "total": {"$sum": "$tickets.flight_legs.amount"}
        }},
        {"$sort": {"total": -1}},
        {"$limit": 20},
        {"$project": {
            "route": "$_id.route",
            "fare_conditions": "$_id.cond",
            "total_revenue_by_class": "$total",
            "_id": 0
        }}
    ]
    complex_rev, log4 = execute_nosql_and_time(mongo.db.bookings, pipeline_complex, label="Rev by Route & Class")

    return jsonify({
        'revenue_by_class': rev_by_class, 
        'top_revenue_routes': top_rev, 
        'least_revenue_routes': low_rev, 
        'revenue_by_class_route': complex_rev, 
        '_perf': [log1, log2, log3, log4]
    })

@app.route('/api/nosql/aircraft-routes/<aircraft_code>')
def nosql_get_aircraft_routes(aircraft_code):
    try:
        pipeline = [
            # 1. MATCH: Filter by Aircraft Code
            {"$match": {
                "aircraft.code": aircraft_code
            }},
            # 2. PROJECT: Convert to strings immediately to prevent type errors
            {"$project": {
                "flight_id": {"$toString": "$_id"},
                # Handle potential missing/null airport codes safely
                "dep_code": {"$ifNull": ["$departure.airport_code", "???"]},
                "arr_code": {"$ifNull": ["$arrival.airport_code", "???"]},
                # Convert date to string first, so $substr never crashes
                "sched_dep_str": {"$ifNull": [{"$toString": "$scheduled_departure"}, ""]},
                "status": {"$ifNull": ["$status", "Unknown"]},
                "_id": 0
            }},
            # 3. FORMAT: Construct the final fields
            {"$project": {
                "flight_id": 1,
                "route": {"$concat": ["$dep_code", " -> ", "$arr_code"]},
                "scheduled_departure_time": {
                    "$cond": {
                        "if": {"$gte": [{"$strLenCP": "$sched_dep_str"}, 16]},
                        "then": {"$substrCP": ["$sched_dep_str", 0, 16]},
                        "else": "$sched_dep_str"
                    }
                },
                "status": 1
            }},
            {"$sort": {"scheduled_departure_time": 1}},
            {"$limit": 50}
        ]
        
        # BYPASSING execute_nosql_and_time to prevent logger crashes
        # We run the aggregate directly to ensure data loads
        routes = list(mongo.db.flights.aggregate(pipeline))
        
        return jsonify({'routes': routes})

    except Exception as e:
        print(f"Error in nosql_get_aircraft_routes: {e}")
        return jsonify({'error': str(e)}), 500

# --- NoSQL CRUD Endpoints ---

@app.route('/api/nosql/flights/<id>', methods=['GET'])
def get_nosql_flight_single(id):
    try: query_id = int(id)
    except: query_id = ObjectId(id)
    
    doc = mongo.db.flights.find_one({'_id': query_id})
    if not doc: return jsonify({'error': 'Flight not found'}), 404
    
    return jsonify({
        'flight_id': str(doc.get('_id')),
        'flight_no': doc.get('flight_no'),
        'scheduled_departure': doc.get('scheduled_departure'),
        'scheduled_arrival': doc.get('scheduled_arrival'),
        'departure_airport': doc.get('departure', {}).get('airport_code'),
        'arrival_airport': doc.get('arrival', {}).get('airport_code'),
        'status': doc.get('status'),
        'aircraft_code': doc.get('aircraft', {}).get('code'),
        'actual_departure': doc.get('actual_departure'),
        'actual_arrival': doc.get('actual_arrival'),
        'version': doc.get('version', 1)
    })

@app.route('/api/nosql/flights', methods=['GET'])
def get_nosql_flights():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    search = request.args.get('search', '')
    column = request.args.get('column', 'flight_no')
    
    query = {}
    if search:
        if column == 'flight_id':
             query = {'_id': int(search) if search.isdigit() else search}
        elif column == 'route':
            query = {
                "$or": [
                    {"departure.airport_code": {"$regex": search, "$options": "i"}},
                    {"arrival.airport_code": {"$regex": search, "$options": "i"}}
                ]
            }
        else:
            db_field = column
            if column == 'aircraft_code': db_field = 'aircraft.code'
            query = {db_field: {"$regex": search, "$options": "i"}}
        
    total = mongo.db.flights.count_documents(query)
    cursor = mongo.db.flights.find(query).skip((page-1)*per_page).limit(per_page)
    
    flights = []
    for doc in cursor:
        flights.append({
            'flight_id': str(doc.get('_id')),
            'flight_no': doc.get('flight_no'),
            'scheduled_departure': doc.get('scheduled_departure'),
            'scheduled_arrival': doc.get('scheduled_arrival'),
            'departure_airport': doc.get('departure', {}).get('airport_code'),
            'arrival_airport': doc.get('arrival', {}).get('airport_code'),
            'status': doc.get('status'),
            'aircraft_code': doc.get('aircraft', {}).get('code'),
            'actual_departure': doc.get('actual_departure'),
            'actual_arrival': doc.get('actual_arrival'),
            'version': doc.get('version', 1) # Return version for concurrency
        })
    return jsonify({'flights': flights, 'total': total, 'page': page, 'total_pages': (total + per_page - 1) // per_page})

@app.route('/api/nosql/flights', methods=['POST'])
def create_nosql_flight():
    data = request.get_json()
    new_flight = {
        'flight_no': data['flight_no'],
        'scheduled_departure': data['scheduled_departure'],
        'scheduled_arrival': data['scheduled_arrival'],
        'status': data.get('status', 'Scheduled'),
        'departure': {'airport_code': data['departure_airport']}, 
        'arrival': {'airport_code': data['arrival_airport']},     
        'aircraft': {'code': data['aircraft_code']},
        'version': 1 # Initialize version
    }
    result = mongo.db.flights.insert_one(new_flight)
    return jsonify({'message': 'Flight created in MongoDB', 'id': str(result.inserted_id)}), 201

@app.route('/api/nosql/flights/<id>', methods=['PUT'])
def update_nosql_flight(id):
    data = request.get_json()
    
    # 1. Get client version
    client_version = data.get('version')
    if client_version is None:
        return jsonify({'error': 'Missing version for concurrency control'}), 400

    update_data = {}
    if 'flight_no' in data: update_data['flight_no'] = data['flight_no']
    if 'status' in data: update_data['status'] = data['status']
    if 'departure_airport' in data: update_data['departure.airport_code'] = data['departure_airport']
    if 'arrival_airport' in data: update_data['arrival.airport_code'] = data['arrival_airport']
    
    try: query_id = int(id)
    except: query_id = ObjectId(id)

    # 2. Atomic Update: Match ID AND Version
    result = mongo.db.flights.update_one(
        {'_id': query_id, 'version': int(client_version)}, 
        {
            '$set': update_data,
            '$inc': {'version': 1} # Increment version on success
        }
    )

    # 3. Handle Result
    if result.matched_count == 0:
        if mongo.db.flights.find_one({'_id': query_id}):
            return jsonify({'error': 'CONCURRENCY CONFLICT: Record modified by another user.'}), 409
        return jsonify({'error': 'Flight not found'}), 404

    return jsonify({'message': 'Flight updated in MongoDB'})

@app.route('/api/nosql/flights/<id>', methods=['DELETE'])
def delete_nosql_flight(id):
    try: query_id = int(id)
    except: query_id = ObjectId(id)
    
    # Capture the result of the delete operation
    result = mongo.db.flights.delete_one({'_id': query_id})
    
    # Check if anything was actually deleted
    if result.deleted_count == 0:
        return jsonify({'error': 'Flight not found or already deleted'}), 404
        
    return jsonify({'message': 'Flight deleted from MongoDB'})

# === NoSQL Booking CRUD (POST/PUT/DELETE) ===

@app.route('/api/nosql/bookings', methods=['GET'])
def get_nosql_bookings_formatted():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    search = request.args.get('search', '')
    column = request.args.get('column', 'ticket_no')
    
    query = {}
    if search:
        if column == 'book_ref':
            query = {'_id': {"$regex": search, "$options": "i"}}
        else:
            query = {f'tickets.{column}': {"$regex": search, "$options": "i"}}

    total = mongo.db.bookings.count_documents(query)
    cursor = mongo.db.bookings.find(query).skip((page-1)*per_page).limit(per_page)
    
    output = []
    for b in cursor:
        tickets = b.get('tickets', [])
        first_ticket = tickets[0] if len(tickets) > 0 else {}
        output.append({
            'ticket_no': first_ticket.get('ticket_no', 'N/A'),
            'book_ref': b.get('_id'),
            'passenger_id': first_ticket.get('passenger_id', 'N/A')
            # UPDATED: Removed name and contact to match SQL
        })
    return jsonify({'bookings': output, 'total': total, 'page': page, 'total_pages': (total + per_page - 1) // per_page})

@app.route('/api/nosql/bookings/<ticket_no>', methods=['GET'])
def get_nosql_booking_single(ticket_no):
    booking = mongo.db.bookings.find_one({"tickets.ticket_no": ticket_no})
    if not booking: return jsonify({'error': 'Booking not found'}), 404
    
    ticket_data = next((t for t in booking.get('tickets', []) if t['ticket_no'] == ticket_no), {})
    
    return jsonify({
        'ticket_no': ticket_no,
        'book_ref': booking['_id'],
        'passenger_id': ticket_data.get('passenger_id'),
        'version': booking.get('version', 1)
        # UPDATED: Removed name and contact
    })

@app.route('/api/nosql/bookings', methods=['POST'])
def create_nosql_booking():
    data = request.get_json()
    # UPDATED: Only strictly required fields
    new_ticket = {
        'ticket_no': data['ticket_no'],
        'passenger_id': data['passenger_id'],
        'flight_legs': [] 
    }
    existing = mongo.db.bookings.find_one({'_id': data['book_ref']})
    if existing:
        mongo.db.bookings.update_one(
            {'_id': data['book_ref']}, 
            {
                '$push': {'tickets': new_ticket},
                '$inc': {'version': 1}
            }
        )
        return jsonify({'message': 'Added ticket to existing booking'}), 201
    else:
        new_booking = {
            '_id': data['book_ref'],
            'book_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'total_amount': 0,
            'tickets': [new_ticket],
            'version': 1
        }
        mongo.db.bookings.insert_one(new_booking)
        return jsonify({'message': 'Created new booking with ticket'}), 201

@app.route('/api/nosql/bookings/<ticket_no>', methods=['PUT'])
def update_nosql_booking(ticket_no):
    data = request.get_json()
    client_version = data.get('version')
    if client_version is None: return jsonify({'error': 'Missing version'}), 400

    update_fields = {}
    if 'passenger_id' in data: update_fields['tickets.$.passenger_id'] = data['passenger_id']
    
    update_fields['version'] = int(client_version) + 1

    result = mongo.db.bookings.update_one(
        {'tickets.ticket_no': ticket_no, 'version': int(client_version)}, 
        {'$set': update_fields}
    )
    
    if result.matched_count == 0:
        if mongo.db.bookings.find_one({'tickets.ticket_no': ticket_no}):
             return jsonify({'error': 'CONCURRENCY CONFLICT: Booking modified by another user.'}), 409
        return jsonify({'error': 'Ticket not found'}), 404
        
    return jsonify({'message': 'Booking updated'})

@app.route('/api/nosql/bookings/<ticket_no>', methods=['DELETE'])
def delete_nosql_booking(ticket_no):
    # 1. Find the parent booking document first
    booking = mongo.db.bookings.find_one({"tickets.ticket_no": ticket_no})
    
    # If the booking doesn't exist or ticket isn't in it -> 404
    if not booking: 
        return jsonify({'error': 'Ticket not found or already deleted'}), 404
    
    book_ref = booking['_id']
    
    # 2. Attempt to remove the ticket (Atomic Pull)
    result = mongo.db.bookings.update_one(
        {'_id': book_ref, 'tickets.ticket_no': ticket_no}, 
        {
            '$pull': {'tickets': {'ticket_no': ticket_no}},
            '$inc': {'version': 1} # Important: Increment version of parent doc
        }
    )
    
    # 3. Check if anything was actually removed
    if result.modified_count == 0:
        return jsonify({'error': 'Ticket already deleted by another user'}), 404
    
    # 4. Cleanup: If the booking has no more tickets, delete the booking document
    updated = mongo.db.bookings.find_one({'_id': book_ref})
    if updated and len(updated.get('tickets', [])) == 0:
        mongo.db.bookings.delete_one({'_id': book_ref})
        return jsonify({'message': 'Ticket deleted and empty booking removed'})
        
    return jsonify({'message': 'Ticket deleted from booking'})

@app.route('/api/nosql/aircraft/<id>', methods=['GET'])
def get_nosql_aircraft_single(id):
    doc = mongo.db.aircrafts.find_one({'_id': id})
    if not doc: return jsonify({'error': 'Aircraft not found'}), 404
    
    return jsonify({
        'aircraft_code': doc['_id'],
        'model': doc.get('model', 'Unknown'),
        'range': doc.get('range', 0),
        'version': doc.get('version', 1)
    })

@app.route('/api/nosql/aircraft', methods=['GET'])
def get_nosql_aircraft():
    page = int(request.args.get('page', 1))
    per_page = 20
    search = request.args.get('search', '')
    column = request.args.get('column', 'aircraft_code')

    query = {}
    if search:
        db_field = column
        if column == 'aircraft_code': db_field = '_id' 
        query = {db_field: {"$regex": search, "$options": "i"}}

    total = mongo.db.aircrafts.count_documents(query)
    cursor = mongo.db.aircrafts.find(query).skip((page-1)*per_page).limit(per_page)
    
    aircraft = []
    for doc in cursor:
        aircraft.append({
            'aircraft_code': doc['_id'],
            'model': doc.get('model', 'Unknown'),
            'range': doc.get('range', 0),
            'version': doc.get('version', 1) # Return version
        })
    return jsonify({'aircraft': aircraft, 'total': total, 'page': page, 'total_pages': (total//per_page)+1})

@app.route('/api/nosql/aircraft', methods=['POST'])
def create_nosql_aircraft():
    data = request.get_json()
    new_ac = {
        '_id': data['aircraft_code'], 
        'model': data['model'], 
        'range': data['range'],
        'version': 1 # Initialize
    }
    try:
        mongo.db.aircrafts.insert_one(new_ac)
        return jsonify({'message': 'Aircraft created'}), 201
    except:
        return jsonify({'error': 'Duplicate or Error'}), 400

@app.route('/api/nosql/aircraft/<id>', methods=['PUT'])
def update_nosql_aircraft(id):
    data = request.get_json()
    client_version = data.get('version')
    if client_version is None: return jsonify({'error': 'Missing version'}), 400

    result = mongo.db.aircrafts.update_one(
        {'_id': id, 'version': int(client_version)}, 
        {
            '$set': {'range': data.get('range'), 'model': data.get('model')}, # Added model update
            '$inc': {'version': 1}
        }
    )
    if result.matched_count == 0:
         if mongo.db.aircrafts.find_one({'_id': id}):
            return jsonify({'error': 'CONCURRENCY CONFLICT'}), 409
         return jsonify({'error': 'Aircraft not found'}), 404
    return jsonify({'message': 'Aircraft updated'})

@app.route('/api/nosql/aircraft/<id>', methods=['DELETE'])
def delete_nosql_aircraft(id):
    result = mongo.db.aircrafts.delete_one({'_id': id})
    
    if result.deleted_count == 0:
        return jsonify({'error': 'Aircraft not found or already deleted'}), 404
        
    return jsonify({'message': 'Aircraft deleted'})

@app.route('/api/nosql/airports/<id>', methods=['GET'])
def get_nosql_airport_single(id):
    doc = mongo.db.airports.find_one({'_id': id})
    if not doc: return jsonify({'error': 'Airport not found'}), 404
    
    return jsonify({
        'airport_code': doc['_id'],
        'airport_name': doc.get('airport_name', 'Unknown'),
        'city': doc.get('city', 'Unknown'),
        'timezone': doc.get('timezone', ''),
        'coordinates': doc.get('coordinates', ''),
        'version': doc.get('version', 1)
    })

@app.route('/api/nosql/airports', methods=['GET'])
def get_nosql_airports():
    page = int(request.args.get('page', 1))
    per_page = 20
    search = request.args.get('search', '')
    column = request.args.get('column', 'airport_code')

    query = {}
    if search:
        db_field = column
        if column == 'airport_code': db_field = '_id'
        query = {db_field: {"$regex": search, "$options": "i"}}
        
    total = mongo.db.airports.count_documents(query)
    cursor = mongo.db.airports.find(query).skip((page-1)*per_page).limit(per_page)
    
    airports = []
    for doc in cursor:
        airports.append({
            'airport_code': doc['_id'],
            'airport_name': doc.get('airport_name', 'Unknown'),
            'city': doc.get('city', 'Unknown'),
            'timezone': doc.get('timezone', ''),
            'coordinates': doc.get('coordinates', ''),
            'version': doc.get('version', 1) # Return version
        })
    return jsonify({'airports': airports, 'total': total, 'page': page, 'total_pages': (total//per_page)+1})

@app.route('/api/nosql/airports', methods=['POST'])
def create_nosql_airport():
    data = request.get_json()
    new_ap = {
        '_id': data['airport_code'], 
        'airport_name': data['airport_name'], 
        'city': data['city'], 
        'timezone': data.get('timezone'), 
        'coordinates': data.get('coordinates'),
        'version': 1 # Initialize
    }
    try:
        mongo.db.airports.insert_one(new_ap)
        return jsonify({'message': 'Airport created'}), 201
    except:
        return jsonify({'error': 'Duplicate or Error'}), 400

@app.route('/api/nosql/airports/<id>', methods=['PUT'])
def update_nosql_airport(id):
    data = request.get_json()
    client_version = data.get('version')
    if client_version is None: return jsonify({'error': 'Missing version'}), 400

    update = {k: v for k, v in data.items() if k != 'version'}
    
    result = mongo.db.airports.update_one(
        {'_id': id, 'version': int(client_version)}, 
        {'$set': update, '$inc': {'version': 1}}
    )
    if result.matched_count == 0:
         if mongo.db.airports.find_one({'_id': id}):
            return jsonify({'error': 'CONCURRENCY CONFLICT'}), 409
         return jsonify({'error': 'Airport not found'}), 404
    return jsonify({'message': 'Airport updated'})

@app.route('/api/nosql/airports/<id>', methods=['DELETE'])
def delete_nosql_airport(id):
    result = mongo.db.airports.delete_one({'_id': id})
    
    if result.deleted_count == 0:
        return jsonify({'error': 'Airport not found or already deleted'}), 404
        
    return jsonify({'message': 'Airport deleted'})

if __name__ == '__main__':
    try:
        init_db()
        print("\n" + "="*50)
        print("Starting Flask server...")
        print(f"Database location: {SQLITE_DB}")
        print("="*50 + "\n")
        app.run(debug=True, port=5000)
    except Exception as e:
        print(f"Failed to start application: {e}")