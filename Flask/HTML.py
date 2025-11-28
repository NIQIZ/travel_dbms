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

# === SQL PERFORMANCE HELPER ===
def execute_and_time(cursor, query, params=(), label="Query"):
    print(f"\n--- SQL PERFORMANCE: {label} ---")
    try:
        explain_query = f"EXPLAIN QUERY PLAN {query}"
        cursor.execute(explain_query, params)
        plan = cursor.fetchall()
        print(f"B+ Tree Usage:")
        for row in plan:
            print(f"  {row['detail']}")
    except Exception as e:
        print(f"  Could not explain plan: {e}")

    start_time = time.perf_counter()
    cursor.execute(query, params)
    results = cursor.fetchall()
    duration_ms = (time.perf_counter() - start_time) * 1000
    print(f"Execution Time: {duration_ms:.4f} ms")
    print("-" * 50)
    return [dict(row) for row in results]

# === NOSQL PERFORMANCE HELPER (NEW) ===
def execute_nosql_and_time(collection, pipeline, label="NoSQL Query"):
    print(f"\n--- NOSQL PERFORMANCE: {label} ---")
    
    # 1. Verify Index Usage (Explain Plan)
    try:
        # We use the 'explain' command on the pipeline
        explanation = mongo.db.command(
            'aggregate', collection.name,
            pipeline=pipeline,
            explain=True
        )
        
        # Convert to string to search for keywords (Parsing deep JSON is complex)
        plan_str = json.dumps(explanation)
        
        print("Index Usage Verification:")
        if "IXSCAN" in plan_str:
            print("  YES: Used Index Scan (IXSCAN)")
        elif "COLLSCAN" in plan_str:
            print("  NO: Used Collection Scan (COLLSCAN - Slow)")
        else:
            print("  Complex Stage (See raw explain for details)")
            
    except Exception as e:
        print(f"  Could not explain plan: {e}")

    # 2. Measure Execution Time
    start_time = time.perf_counter()
    
    # We must convert to list() to force actual execution
    results = list(collection.aggregate(pipeline))
    
    duration_ms = (time.perf_counter() - start_time) * 1000
    print(f"Execution Time: {duration_ms:.4f} ms")
    print("-" * 50)
    
    return results

def extract_json_value(json_str):
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

# ==================== SQL ANALYTICS (With Timing) ====================

@app.route('/api/flight-operations')
def flight_operations():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        q1 = "SELECT route, ROUND(AVG((JULIANDAY(SUBSTR(actual_arrival, 1, 19)) - JULIANDAY(SUBSTR(scheduled_arrival, 1, 19))) * 24 * 60), 2) AS avg_delay_mins FROM flight_routes WHERE status = 'Arrived' AND actual_arrival IS NOT NULL AND scheduled_arrival IS NOT NULL AND JULIANDAY(SUBSTR(actual_arrival, 1, 19)) > JULIANDAY(SUBSTR(scheduled_arrival, 1, 19)) GROUP BY route HAVING avg_delay_mins > 0 ORDER BY avg_delay_mins DESC LIMIT 5;"
        least_punctual_routes = execute_and_time(cursor, q1, label="Least Punctual Routes")
        
        q2 = "SELECT COUNT(*) as total_flights, SUM(CASE WHEN status LIKE '%Delayed%' OR status = 'Delayed' THEN 1 ELSE 0 END) as delayed_flights, SUM(CASE WHEN status LIKE '%Cancel%' OR status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled_flights, SUM(CASE WHEN status = 'Arrived' OR status = 'On Time' THEN 1 ELSE 0 END) as ontime_flights FROM flights"
        overview_res = execute_and_time(cursor, q2, label="Overview Metrics")
        overview = overview_res[0] if overview_res else {}
        
        q3 = "SELECT ROUND(AVG((JULIANDAY(SUBSTR(actual_arrival, 1, 19)) - JULIANDAY(SUBSTR(scheduled_arrival, 1, 19))) * 24 * 60), 2) as avg_delay_minutes FROM flights WHERE actual_arrival IS NOT NULL AND scheduled_arrival IS NOT NULL AND status = 'Arrived' AND JULIANDAY(SUBSTR(actual_arrival, 1, 19)) > JULIANDAY(SUBSTR(scheduled_arrival, 1, 19))"
        delay_res = execute_and_time(cursor, q3, label="Avg Delay Calculation")
        overview['avg_delay_minutes'] = delay_res[0]['avg_delay_minutes'] if delay_res and delay_res[0]['avg_delay_minutes'] else 0
        
        conn.close()
        return jsonify({'least_punctual_routes': least_punctual_routes, 'overview': overview})
    except Exception as e: return jsonify({'error': str(e)}), 500

@app.route('/api/route-performance')
def route_performance():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = "SELECT fr.route, COUNT(fr.flight_id) AS flight_count FROM flight_routes AS fr GROUP BY fr.route ORDER BY flight_count DESC LIMIT 10;"
        busiest_routes = execute_and_time(cursor, query, label="Route Performance")
        conn.close()
        return jsonify({'busiest_routes': busiest_routes})
    except Exception as e: return jsonify({'error': str(e)}), 500

@app.route('/api/passenger-demand')
def passenger_demand():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        q1 = "WITH FlightCapacity AS (SELECT fr.flight_id, fr.route, COUNT(s.seat_no) AS total_seats FROM flight_routes AS fr JOIN seats AS s ON fr.aircraft_code = s.aircraft_code GROUP BY fr.flight_id, fr.route), FlightBookings AS (SELECT flight_id, COUNT(ticket_no) AS booked_seats FROM boarding_passes GROUP BY flight_id) SELECT fc.route, ROUND(AVG((fb.booked_seats * 100.0 / fc.total_seats)), 2) AS avg_occupancy_percent FROM FlightCapacity AS fc JOIN FlightBookings AS fb ON fc.flight_id = fb.flight_id WHERE fc.total_seats > 0 GROUP BY fc.route ORDER BY avg_occupancy_percent DESC LIMIT 10;"
        top_occupancy_routes = execute_and_time(cursor, q1, label="Passenger Occupancy")
        q2 = "WITH RouteBookings AS (SELECT fr.route, COUNT(tf.ticket_no) AS total_tickets_sold FROM flight_routes AS fr JOIN ticket_flights AS tf ON fr.flight_id = tf.flight_id GROUP BY fr.route), TotalTickets AS (SELECT CAST(COUNT(ticket_no) AS REAL) AS grand_total FROM ticket_flights) SELECT rb.route, rb.total_tickets_sold, ROUND((rb.total_tickets_sold * 100.0 / tt.grand_total), 2) AS market_share_percent FROM RouteBookings AS rb CROSS JOIN TotalTickets AS tt ORDER BY market_share_percent DESC LIMIT 10;"
        busiest_routes_market_share = execute_and_time(cursor, q2, label="Market Share High")
        q3 = "WITH RouteBookings AS (SELECT fr.route, COUNT(tf.ticket_no) AS total_tickets_sold FROM flight_routes AS fr JOIN ticket_flights AS tf ON fr.flight_id = tf.flight_id GROUP BY fr.route), TotalTickets AS (SELECT CAST(COUNT(ticket_no) AS REAL) AS grand_total FROM ticket_flights) SELECT rb.route, rb.total_tickets_sold, ROUND((rb.total_tickets_sold * 100.0 / tt.grand_total), 2) AS market_share_percent FROM RouteBookings AS rb CROSS JOIN TotalTickets AS tt WHERE rb.total_tickets_sold > 0 ORDER BY market_share_percent ASC LIMIT 10;"
        least_busy_routes = execute_and_time(cursor, q3, label="Market Share Low")
        conn.close()
        return jsonify({'top_occupancy_routes': top_occupancy_routes, 'busiest_routes_market_share': busiest_routes_market_share, 'least_busy_routes': least_busy_routes})
    except Exception as e: return jsonify({'error': str(e)}), 500

@app.route('/api/revenue-analysis')
def revenue_analysis():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        q1 = "SELECT tf.fare_conditions, SUM(tf.amount) AS total_revenue_by_class FROM ticket_flights AS tf GROUP BY tf.fare_conditions ORDER BY total_revenue_by_class DESC;"
        revenue_by_class = execute_and_time(cursor, q1, label="Revenue by Class")
        q2 = "SELECT fr.route, SUM(tf.amount) AS total_revenue FROM flight_routes AS fr JOIN ticket_flights AS tf ON fr.flight_id = tf.flight_id GROUP BY fr.route ORDER BY total_revenue DESC LIMIT 3;"
        top_revenue_routes = execute_and_time(cursor, q2, label="Top Revenue Routes")
        q3 = "SELECT fr.route, SUM(tf.amount) AS total_revenue FROM flight_routes AS fr JOIN ticket_flights AS tf ON fr.flight_id = tf.flight_id GROUP BY fr.route HAVING total_revenue > 0 ORDER BY total_revenue ASC LIMIT 3;"
        least_revenue_routes = execute_and_time(cursor, q3, label="Least Revenue Routes")
        q4 = "SELECT fr.route, tf.fare_conditions, SUM(tf.amount) AS total_revenue_by_class FROM flight_routes AS fr JOIN ticket_flights AS tf ON fr.flight_id = tf.flight_id GROUP BY fr.route, tf.fare_conditions ORDER BY total_revenue_by_class DESC LIMIT 20;"
        revenue_by_class_route = execute_and_time(cursor, q4, label="Rev by Route & Class")
        conn.close()
        return jsonify({'revenue_by_class': revenue_by_class, 'top_revenue_routes': top_revenue_routes, 'least_revenue_routes': least_revenue_routes, 'revenue_by_class_route': revenue_by_class_route})
    except Exception as e: return jsonify({'error': str(e)}), 500

@app.route('/api/resource-planning')
def resource_planning():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        q1 = "SELECT fr.route, fr.aircraft_code, ad.model AS aircraft_model, COUNT(fr.flight_id) AS total_flights_on_route FROM flight_routes AS fr JOIN aircrafts_data AS ad ON fr.aircraft_code = ad.aircraft_code GROUP BY fr.route, fr.aircraft_code, ad.model ORDER BY total_flights_on_route DESC LIMIT 20;"
        aircraft_by_route_raw = execute_and_time(cursor, q1, label="Aircraft by Route")
        aircraft_by_route = []
        for row in aircraft_by_route_raw:
            d = row
            d['aircraft_model'] = extract_json_value(row['aircraft_model'])
            aircraft_by_route.append(d)
        q2 = "SELECT f.arrival_airport AS airport_code, ad.city AS destination_city, COUNT(f.flight_id) AS total_arrivals FROM flights AS f JOIN airports_data AS ad ON f.arrival_airport = ad.airport_code GROUP BY f.arrival_airport, destination_city ORDER BY total_arrivals DESC LIMIT 3;"
        destinations_raw = execute_and_time(cursor, q2, label="Top Destinations")
        top_destinations = []
        for row in destinations_raw:
            top_destinations.append({'airport_code': row['airport_code'], 'city': extract_json_value(row['destination_city']), 'total_arrivals': row['total_arrivals']})
        q3 = "SELECT f.aircraft_code, ad.model AS aircraft_model, SUM(ad.range) AS total_utilization_proxy_miles FROM flights AS f JOIN aircrafts_data AS ad ON f.aircraft_code = ad.aircraft_code WHERE f.status = 'Arrived' GROUP BY f.aircraft_code, aircraft_model ORDER BY total_utilization_proxy_miles DESC LIMIT 10;"
        aircraft_utilization_raw = execute_and_time(cursor, q3, label="Aircraft Utilization")
        aircraft_utilization = []
        for row in aircraft_utilization_raw:
            aircraft_utilization.append({'aircraft_code': row['aircraft_code'], 'aircraft_model': extract_json_value(row['aircraft_model']), 'total_mileage': row['total_utilization_proxy_miles']})
        q4 = "SELECT DISTINCT f.aircraft_code, ad.model AS aircraft_model, COUNT(f.flight_id) as flight_count FROM flights AS f JOIN aircrafts_data AS ad ON f.aircraft_code = ad.aircraft_code GROUP BY f.aircraft_code, ad.model ORDER BY flight_count DESC;"
        aircraft_list_raw = execute_and_time(cursor, q4, label="Aircraft List")
        aircraft_list = []
        for row in aircraft_list_raw:
            aircraft_list.append({'aircraft_code': row['aircraft_code'], 'aircraft_model': extract_json_value(row['aircraft_model']), 'flight_count': row['flight_count']})
        q5 = "SELECT fr.flight_id, fr.route, SUBSTR(fr.scheduled_departure, 1, 16) AS scheduled_departure_time, fr.status FROM flight_routes AS fr WHERE fr.aircraft_code = 'SU9' ORDER BY fr.scheduled_departure ASC LIMIT 50;"
        su9_routes = execute_and_time(cursor, q5, label="SU9 Routes")
        su9_routes_list = [dict(row) for row in su9_routes] 
        conn.close()
        return jsonify({'aircraft_by_route': aircraft_by_route, 'top_destinations': top_destinations, 'aircraft_utilization': aircraft_utilization, 'aircraft_list': aircraft_list, 'su9_routes': su9_routes_list})
    except Exception as e: return jsonify({'error': str(e)}), 500

@app.route('/api/aircraft-routes/<aircraft_code>')
def get_aircraft_routes(aircraft_code):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT fr.flight_id, fr.route, SUBSTR(fr.scheduled_departure, 1, 16) AS scheduled_departure_time, fr.status FROM flight_routes AS fr WHERE fr.aircraft_code = ? ORDER BY fr.scheduled_departure ASC LIMIT 50;", (aircraft_code,))
        routes = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify({'routes': routes})
    except Exception as e: return jsonify({'error': str(e)}), 500

# ==================== CRUD MANAGEMENT PAGES ====================
@app.route('/add_booking', methods=['GET', 'POST'])
def add_booking(): return render_template('add_booking.html', success=False)

@app.route('/crudManager')
def manage(): return render_template('crudManager.html')

# ==================== SQL CRUD (Transaction Managed) ====================
# (Skipping SQL CRUD Logic display for brevity - assumed same as previous)
# ... [SQL CRUD FUNCTIONS] ...
@app.route('/api/flights', methods=['GET'])
def get_flights():
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        search = request.args.get('search', '', type=str)
        conn = get_db_connection()
        cursor = conn.cursor()
        if search:
            cursor.execute("SELECT COUNT(*) FROM flights WHERE flight_id LIKE ? OR departure_airport LIKE ? OR arrival_airport LIKE ?", (f'%{search}%', f'%{search}%', f'%{search}%'))
        else:
            cursor.execute("SELECT COUNT(*) FROM flights")
        total = cursor.fetchone()[0]
        offset = (page - 1) * per_page
        if search:
            cursor.execute("SELECT * FROM flights WHERE flight_id LIKE ? OR departure_airport LIKE ? OR arrival_airport LIKE ? ORDER BY scheduled_departure DESC LIMIT ? OFFSET ?", (f'%{search}%', f'%{search}%', f'%{search}%', per_page, offset))
        else:
            cursor.execute("SELECT * FROM flights ORDER BY scheduled_departure DESC LIMIT ? OFFSET ?", (per_page, offset))
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
        conn = get_db_connection()
        cursor = conn.cursor()
        base_query = "SELECT t.ticket_no, t.book_ref, t.passenger_id, b.book_date, 'Unknown Passenger' as passenger_name, '{}' as contact_data FROM tickets t JOIN bookings b ON t.book_ref = b.book_ref"
        if search:
            cursor.execute("SELECT COUNT(*) FROM tickets t JOIN bookings b ON t.book_ref = b.book_ref WHERE t.ticket_no LIKE ? OR t.book_ref LIKE ?", (f'%{search}%', f'%{search}%'))
        else:
            cursor.execute("SELECT COUNT(*) FROM tickets")
        total = cursor.fetchone()[0]
        offset = (page - 1) * per_page
        if search:
            sql = f"{base_query} WHERE t.ticket_no LIKE ? OR t.book_ref LIKE ? ORDER BY b.book_date DESC LIMIT ? OFFSET ?"
            cursor.execute(sql, (f'%{search}%', f'%{search}%', per_page, offset))
        else:
            sql = f"{base_query} ORDER BY b.book_date DESC LIMIT ? OFFSET ?"
            cursor.execute(sql, (per_page, offset))
        bookings = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify({'bookings': bookings, 'total': total, 'page': page, 'per_page': per_page, 'total_pages': (total + per_page - 1) // per_page})
    except Exception as e:
        print(f"SQL Error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/bookings/<ticket_no>', methods=['GET'])
def get_booking(ticket_no):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tickets WHERE ticket_no = ?", (ticket_no,))
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
        required_fields = ['ticket_no', 'book_ref', 'passenger_id', 'passenger_name', 'contact_data']
        for field in required_fields:
            if field not in data: return jsonify({'error': f'Missing required field: {field}'}), 400
        conn = get_db_connection()
        cursor = conn.cursor()
        contact_data = data['contact_data']
        if isinstance(contact_data, dict): contact_data = json.dumps(contact_data)
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute("INSERT INTO tickets (ticket_no, book_ref, passenger_id, passenger_name, contact_data) VALUES (?, ?, ?, ?, ?)", (data['ticket_no'], data['book_ref'], data['passenger_id'], data['passenger_name'], contact_data))
        conn.commit()
        return jsonify({'message': 'Booking created successfully'}), 201
    except sqlite3.IntegrityError as e:
        if conn: conn.rollback()
        return jsonify({'error': f'Database integrity error: {str(e)}'}), 400
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
        for field in ['passenger_name', 'contact_data']:
            if field in data:
                val = data[field]
                if field == 'contact_data' and isinstance(val, dict): val = json.dumps(val)
                update_fields.append(f"{field} = ?")
                values.append(val)
        if not update_fields: return jsonify({'error': 'No fields'}), 400
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
        conn = get_db_connection()
        cursor = conn.cursor()
        if search:
            cursor.execute("SELECT COUNT(*) FROM aircrafts_data WHERE aircraft_code LIKE ?", (f'%{search}%',))
        else:
            cursor.execute("SELECT COUNT(*) FROM aircrafts_data")
        total = cursor.fetchone()[0]
        offset = (page - 1) * per_page
        if search:
            cursor.execute("SELECT * FROM aircrafts_data WHERE aircraft_code LIKE ? LIMIT ? OFFSET ?", (f'%{search}%', per_page, offset))
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

@app.route('/api/airports', methods=['GET'])
def get_airports():
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        search = request.args.get('search', '', type=str)
        conn = get_db_connection()
        cursor = conn.cursor()
        if search:
            cursor.execute("SELECT COUNT(*) FROM airports_data WHERE airport_code LIKE ? OR airport_name LIKE ? OR city LIKE ?", (f'%{search}%', f'%{search}%', f'%{search}%'))
        else:
            cursor.execute("SELECT COUNT(*) FROM airports_data")
        total = cursor.fetchone()[0]
        offset = (page - 1) * per_page
        if search:
            cursor.execute("SELECT * FROM airports_data WHERE airport_code LIKE ? OR airport_name LIKE ? OR city LIKE ? LIMIT ? OFFSET ?", (f'%{search}%', f'%{search}%', f'%{search}%', per_page, offset))
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
    pipeline_overview = [
        {"$group": {
            "_id": None,
            "total_flights": {"$sum": 1},
            "delayed": {"$sum": {"$cond": [{"$regexMatch": {"input": "$status", "regex": "Delayed"}}, 1, 0]}},
            "cancelled": {"$sum": {"$cond": [{"$regexMatch": {"input": "$status", "regex": "Cancel"}}, 1, 0]}},
            "ontime": {"$sum": {"$cond": [{"$in": ["$status", ["Arrived", "On Time"]]}, 1, 0]}}
        }}
    ]
    overview_stats = execute_nosql_and_time(mongo.db.flights, pipeline_overview, label="Overview Metrics")
    overview = overview_stats[0] if overview_stats else {"total_flights": 0}
    overview_data = {
        "total_flights": overview.get('total_flights', 0),
        "delayed_flights": overview.get('delayed', 0),
        "cancelled_flights": overview.get('cancelled', 0),
        "ontime_flights": overview.get('ontime', 0),
        "avg_delay_minutes": 0 
    }

    pipeline_punctual = [
        {"$match": {"status": "Delayed"}},
        {"$group": {"_id": "$flight_no", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    punctual_data = [{"route": str(doc["_id"]), "avg_delay_mins": doc["count"] * 15} for doc in execute_nosql_and_time(mongo.db.flights, pipeline_punctual, label="Least Punctual")]

    return jsonify({'least_punctual_routes': punctual_data, 'overview': overview_data})

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
    busiest = [{"route": doc["_id"], "flight_count": doc["flight_count"]} for doc in execute_nosql_and_time(mongo.db.flights, pipeline, label="Route Performance")]
    return jsonify({'busiest_routes': busiest})

@app.route('/api/nosql/resource-planning')
def nosql_resource_planning():
    # 1. Aircraft by Route
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
        {"$limit": 20}
    ]
    aircraft_by_route = execute_nosql_and_time(mongo.db.flights, pipeline_aircraft_route, label="Aircraft by Route")

    # 2. Top Destinations
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
    top_destinations = execute_nosql_and_time(mongo.db.flights, pipeline_destinations, label="Top Destinations")

    # 3. Aircraft Utilization
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
    aircraft_utilization = execute_nosql_and_time(mongo.db.flights, pipeline_utilization, label="Aircraft Utilization")

    # 4. Dropdown List
    pipeline_list = [
        {"$group": {"_id": {"code": "$aircraft.code", "model": "$aircraft.model"}, "count": {"$sum": 1}}},
        {"$project": {"aircraft_code": "$_id.code", "aircraft_model": "$_id.model", "flight_count": "$count", "_id": 0}},
        {"$sort": {"flight_count": -1}}
    ]
    aircraft_list = execute_nosql_and_time(mongo.db.flights, pipeline_list, label="Aircraft List")

    return jsonify({
        'aircraft_by_route': aircraft_by_route,
        'top_destinations': top_destinations,
        'aircraft_utilization': aircraft_utilization,
        'aircraft_list': aircraft_list,
        'su9_routes': []
    })

# Stubs for unused charts
@app.route('/api/nosql/passenger-demand')
def nosql_passenger_demand(): return jsonify({'top_occupancy_routes': [], 'busiest_routes_market_share': [], 'least_busy_routes': []})

@app.route('/api/nosql/revenue-analysis')
def nosql_revenue_analysis(): return jsonify({'revenue_by_class': [], 'top_revenue_routes': [], 'least_revenue_routes': [], 'revenue_by_class_route': []})

@app.route('/api/nosql/aircraft-routes/<aircraft_code>')
def nosql_get_aircraft_routes(aircraft_code):
    pipeline = [
        {"$match": {"aircraft.code": aircraft_code}},
        {"$project": {
            "flight_id": {"$toString": "$_id"},
            "route": {"$concat": ["$departure.airport_code", " -> ", "$arrival.airport_code"]},
            "scheduled_departure_time": {"$substr": ["$scheduled_departure", 0, 16]}, 
            "status": 1,
            "_id": 0
        }},
        {"$sort": {"scheduled_departure_time": 1}},
        {"$limit": 50}
    ]
    routes = execute_nosql_and_time(mongo.db.flights, pipeline, label="SU9 Routes")
    return jsonify({'routes': routes})

# --- NoSQL CRUD Endpoints ---

@app.route('/api/nosql/flights', methods=['GET'])
def get_nosql_flights():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    search = request.args.get('search', '')
    query = {}
    if search:
        # IMPROVEMENT: Use Text Search instead of Regex
        query = {"$text": {"$search": search}}
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
            'actual_arrival': doc.get('actual_arrival')
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
        'aircraft': {'code': data['aircraft_code']}               
    }
    result = mongo.db.flights.insert_one(new_flight)
    return jsonify({'message': 'Flight created in MongoDB', 'id': str(result.inserted_id)}), 201

@app.route('/api/nosql/flights/<id>', methods=['PUT'])
def update_nosql_flight(id):
    data = request.get_json()
    update_data = {}
    if 'flight_no' in data: update_data['flight_no'] = data['flight_no']
    if 'status' in data: update_data['status'] = data['status']
    if 'departure_airport' in data: update_data['departure.airport_code'] = data['departure_airport']
    if 'arrival_airport' in data: update_data['arrival.airport_code'] = data['arrival_airport']
    try: query_id = int(id)
    except: query_id = ObjectId(id)
    mongo.db.flights.update_one({'_id': query_id}, {'$set': update_data})
    return jsonify({'message': 'Flight updated in MongoDB'})

@app.route('/api/nosql/flights/<id>', methods=['DELETE'])
def delete_nosql_flight(id):
    try: query_id = int(id)
    except: query_id = ObjectId(id)
    mongo.db.flights.delete_one({'_id': query_id})
    return jsonify({'message': 'Flight deleted from MongoDB'})

# === NoSQL Booking CRUD (POST/PUT/DELETE) ===

@app.route('/api/nosql/bookings', methods=['GET'])
def get_nosql_bookings_formatted():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    total = mongo.db.bookings.count_documents({})
    cursor = mongo.db.bookings.find().skip((page-1)*per_page).limit(per_page)
    output = []
    for b in cursor:
        tickets = b.get('tickets', [])
        first_ticket = tickets[0] if len(tickets) > 0 else {}
        output.append({
            'ticket_no': first_ticket.get('ticket_no', 'N/A'),
            'book_ref': b.get('_id'),
            'passenger_name': first_ticket.get('passenger_name', 'Unknown'), # Use stored name
            'passenger_id': first_ticket.get('passenger_id', 'N/A'),
            'contact_data': '{}'
        })
    return jsonify({'bookings': output, 'total': total, 'page': page, 'total_pages': (total + per_page - 1) // per_page})

@app.route('/api/nosql/bookings', methods=['POST'])
def create_nosql_booking():
    data = request.get_json()
    new_ticket = {
        'ticket_no': data['ticket_no'],
        'passenger_id': data['passenger_id'],
        'passenger_name': data['passenger_name'],
        'contact_data': data['contact_data'],
        'flight_legs': [] 
    }
    existing = mongo.db.bookings.find_one({'_id': data['book_ref']})
    if existing:
        mongo.db.bookings.update_one({'_id': data['book_ref']}, {'$push': {'tickets': new_ticket}})
        return jsonify({'message': 'Added ticket to existing booking'}), 201
    else:
        new_booking = {
            '_id': data['book_ref'],
            'book_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'total_amount': 0,
            'tickets': [new_ticket]
        }
        mongo.db.bookings.insert_one(new_booking)
        return jsonify({'message': 'Created new booking with ticket'}), 201

@app.route('/api/nosql/bookings/<ticket_no>', methods=['PUT'])
def update_nosql_booking(ticket_no):
    data = request.get_json()
    update_fields = {}
    if 'passenger_name' in data: update_fields['tickets.$.passenger_name'] = data['passenger_name']
    if 'contact_data' in data: update_fields['tickets.$.contact_data'] = data['contact_data']
    if not update_fields: return jsonify({'error': 'No fields to update'}), 400
    result = mongo.db.bookings.update_one({'tickets.ticket_no': ticket_no}, {'$set': update_fields})
    if result.matched_count == 0: return jsonify({'error': 'Ticket not found'}), 404
    return jsonify({'message': 'Booking updated'})

@app.route('/api/nosql/bookings/<ticket_no>', methods=['DELETE'])
def delete_nosql_booking(ticket_no):
    booking = mongo.db.bookings.find_one({"tickets.ticket_no": ticket_no})
    if not booking: return jsonify({'error': 'Ticket not found'}), 404
    
    book_ref = booking['_id']
    mongo.db.bookings.update_one({'_id': book_ref}, {'$pull': {'tickets': {'ticket_no': ticket_no}}})
    
    # Check if empty and delete
    updated = mongo.db.bookings.find_one({'_id': book_ref})
    if updated and len(updated.get('tickets', [])) == 0:
        mongo.db.bookings.delete_one({'_id': book_ref})
        return jsonify({'message': 'Ticket deleted and empty booking removed'})
        
    return jsonify({'message': 'Ticket deleted from booking'})

# === End NoSQL Booking CRUD ===

@app.route('/api/nosql/aircraft', methods=['GET'])
def get_nosql_aircraft():
    page = int(request.args.get('page', 1))
    per_page = 20
    total = mongo.db.aircrafts.count_documents({})
    cursor = mongo.db.aircrafts.find().skip((page-1)*per_page).limit(per_page)
    aircraft = []
    for doc in cursor:
        aircraft.append({
            'aircraft_code': doc['_id'],
            'model': doc.get('model', 'Unknown'),
            'range': doc.get('range', 0)
        })
    return jsonify({'aircraft': aircraft, 'total': total, 'page': page, 'total_pages': (total//per_page)+1})

@app.route('/api/nosql/aircraft', methods=['POST'])
def create_nosql_aircraft():
    data = request.get_json()
    new_ac = {'_id': data['aircraft_code'], 'model': data['model'], 'range': data['range']}
    try:
        mongo.db.aircrafts.insert_one(new_ac)
        return jsonify({'message': 'Aircraft created'}), 201
    except:
        return jsonify({'error': 'Duplicate or Error'}), 400

@app.route('/api/nosql/aircraft/<id>', methods=['PUT'])
def update_nosql_aircraft(id):
    data = request.get_json()
    mongo.db.aircrafts.update_one({'_id': id}, {'$set': {'range': data.get('range')}})
    return jsonify({'message': 'Aircraft updated'})

@app.route('/api/nosql/aircraft/<id>', methods=['DELETE'])
def delete_nosql_aircraft(id):
    mongo.db.aircrafts.delete_one({'_id': id})
    return jsonify({'message': 'Aircraft deleted'})

@app.route('/api/nosql/airports', methods=['GET'])
def get_nosql_airports():
    page = int(request.args.get('page', 1))
    per_page = 20
    search = request.args.get('search', '')
    query = {}
    if search:
        query = {"$or": [{"_id": {"$regex": search, "$options": "i"}}, {"airport_name": {"$regex": search, "$options": "i"}}, {"city": {"$regex": search, "$options": "i"}}]}
    total = mongo.db.airports.count_documents(query)
    cursor = mongo.db.airports.find(query).skip((page-1)*per_page).limit(per_page)
    airports = []
    for doc in cursor:
        airports.append({
            'airport_code': doc['_id'],
            'airport_name': doc.get('airport_name', 'Unknown'),
            'city': doc.get('city', 'Unknown'),
            'timezone': doc.get('timezone', ''),
            'coordinates': doc.get('coordinates', '')
        })
    return jsonify({'airports': airports, 'total': total, 'page': page, 'total_pages': (total//per_page)+1})

@app.route('/api/nosql/airports', methods=['POST'])
def create_nosql_airport():
    data = request.get_json()
    new_ap = {'_id': data['airport_code'], 'airport_name': data['airport_name'], 'city': data['city'], 'timezone': data.get('timezone'), 'coordinates': data.get('coordinates')}
    try:
        mongo.db.airports.insert_one(new_ap)
        return jsonify({'message': 'Airport created'}), 201
    except:
        return jsonify({'error': 'Duplicate or Error'}), 400

@app.route('/api/nosql/airports/<id>', methods=['PUT'])
def update_nosql_airport(id):
    data = request.get_json()
    update = {}
    if 'airport_name' in data: update['airport_name'] = data['airport_name']
    if 'city' in data: update['city'] = data['city']
    if 'timezone' in data: update['timezone'] = data['timezone']
    if 'coordinates' in data: update['coordinates'] = data['coordinates']
    mongo.db.airports.update_one({'_id': id}, {'$set': update})
    return jsonify({'message': 'Airport updated'})

@app.route('/api/nosql/airports/<id>', methods=['DELETE'])
def delete_nosql_airport(id):
    mongo.db.airports.delete_one({'_id': id})
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