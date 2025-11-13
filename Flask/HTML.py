import sys
import os
from flask import Flask, render_template, url_for, redirect, request, jsonify
import sqlite3
import json
from datetime import datetime
from flask_pymongo import PyMongo

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
        
        conn = sqlite3.connect(SQLITE_DB)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        raise

def extract_json_value(json_str):
    """Extract value from JSON string (for model, city, airport_name fields)"""
    if not json_str:
        return "Unknown"
    try:
        data = json.loads(json_str)
        if isinstance(data, dict):
            return data.get('en', list(data.values())[0] if data else 'Unknown')
        return str(data)
    except:
        return str(json_str)

# Initialize the database view when app starts
def init_db():
    """Initialize database views"""
    try:
        print("Initializing database views...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create view for flight routes
        cursor.execute("DROP VIEW IF EXISTS flight_routes;")
        cursor.execute("""
            CREATE VIEW flight_routes AS
            SELECT 
                f.*,
                f.departure_airport || ' -> ' || f.arrival_airport AS route
            FROM flights f;
        """)
        conn.commit()
        conn.close()
        print("[OK] Database views initialized successfully")
    except Exception as e:
        print(f"Error initializing database: {e}")
        raise

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
    try:
        print("Loading flight operations data...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Top 5 Least Punctual Routes
        cursor.execute("""
            SELECT 
                route,
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
                AND JULIANDAY(SUBSTR(actual_arrival, 1, 19)) > JULIANDAY(SUBSTR(scheduled_arrival, 1, 19))
            GROUP BY route
            HAVING avg_delay_mins > 0
            ORDER BY avg_delay_mins DESC
            LIMIT 5;
        """)
        least_punctual_routes = [dict(row) for row in cursor.fetchall()]
        
        # Overall flight metrics - counts only
        cursor.execute("""
            SELECT 
                COUNT(*) as total_flights,
                SUM(CASE WHEN status LIKE '%Delayed%' OR status = 'Delayed' THEN 1 ELSE 0 END) as delayed_flights,
                SUM(CASE WHEN status LIKE '%Cancel%' OR status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled_flights,
                SUM(CASE WHEN status = 'Arrived' OR status = 'On Time' THEN 1 ELSE 0 END) as ontime_flights
            FROM flights
        """)
        overview = dict(cursor.fetchone())
        
        # Calculate average delay separately with proper timestamp handling
        cursor.execute("""
            SELECT 
                ROUND(AVG(
                    (
                        JULIANDAY(SUBSTR(actual_arrival, 1, 19)) - 
                        JULIANDAY(SUBSTR(scheduled_arrival, 1, 19))
                    ) * 24 * 60
                ), 2) as avg_delay_minutes
            FROM flights
            WHERE actual_arrival IS NOT NULL 
                AND scheduled_arrival IS NOT NULL
                AND status = 'Arrived'
                AND JULIANDAY(SUBSTR(actual_arrival, 1, 19)) > JULIANDAY(SUBSTR(scheduled_arrival, 1, 19))
        """)
        delay_result = cursor.fetchone()
        overview['avg_delay_minutes'] = delay_result['avg_delay_minutes'] if delay_result['avg_delay_minutes'] else 0
        
        conn.close()
        return jsonify({
            'least_punctual_routes': least_punctual_routes,
            'overview': overview
        })
    except Exception as e:
        print(f"Error in flight_operations: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/route-performance')
def route_performance():
    """Get route and airport performance metrics"""
    try:
        print("Loading route performance data...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 5. Top 10 Routes by Flight Volume
        cursor.execute("""
            SELECT
                fr.route,
                COUNT(fr.flight_id) AS flight_count
            FROM
                flight_routes AS fr
            GROUP BY
                fr.route
            ORDER BY
                flight_count DESC
            LIMIT 10;
        """)
        busiest_routes = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        return jsonify({
            'busiest_routes': busiest_routes
        })
    except Exception as e:
        print(f"Error in route_performance: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/passenger-demand')
def passenger_demand():
    """Get passenger demand and load factors"""
    try:
        print("Loading passenger demand data...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 2. Top 10 Routes by Average Occupancy Rate
        cursor.execute("""
            WITH FlightCapacity AS (
                SELECT
                    fr.flight_id,
                    fr.route,
                    COUNT(s.seat_no) AS total_seats
                FROM
                    flight_routes AS fr
                JOIN
                    seats AS s ON fr.aircraft_code = s.aircraft_code
                GROUP BY
                    fr.flight_id, fr.route
            ),
            FlightBookings AS (
                SELECT
                    flight_id,
                    COUNT(ticket_no) AS booked_seats
                FROM
                    boarding_passes
                GROUP BY
                    flight_id
            )
            SELECT
                fc.route,
                ROUND(AVG(
                    (fb.booked_seats * 100.0 / fc.total_seats)
                ), 2) AS avg_occupancy_percent
            FROM
                FlightCapacity AS fc
            JOIN
                FlightBookings AS fb ON fc.flight_id = fb.flight_id
            WHERE
                fc.total_seats > 0
            GROUP BY
                fc.route
            ORDER BY
                avg_occupancy_percent DESC
            LIMIT 10;
        """)
        top_occupancy_routes = [dict(row) for row in cursor.fetchall()]
        
        # 3. Busiest Routes by Total Passenger Volume (Market Share %)
        cursor.execute("""
            WITH RouteBookings AS (
                SELECT
                    fr.route,
                    COUNT(tf.ticket_no) AS total_tickets_sold
                FROM
                    flight_routes AS fr
                JOIN
                    ticket_flights AS tf ON fr.flight_id = tf.flight_id
                GROUP BY
                    fr.route
            ),
            TotalTickets AS (
                SELECT
                    CAST(COUNT(ticket_no) AS REAL) AS grand_total
                FROM
                    ticket_flights
            )
            SELECT
                rb.route,
                rb.total_tickets_sold,
                ROUND((rb.total_tickets_sold * 100.0 / tt.grand_total), 2) AS market_share_percent
            FROM
                RouteBookings AS rb
            CROSS JOIN
                TotalTickets AS tt
            ORDER BY
                market_share_percent DESC
            LIMIT 10;
        """)
        busiest_routes_market_share = [dict(row) for row in cursor.fetchall()]
        
        # 4. 10 Least Busy Routes by Total Passenger Volume (Market Share %)
        cursor.execute("""
            WITH RouteBookings AS (
                SELECT
                    fr.route,
                    COUNT(tf.ticket_no) AS total_tickets_sold
                FROM
                    flight_routes AS fr
                JOIN
                    ticket_flights AS tf ON fr.flight_id = tf.flight_id
                GROUP BY
                    fr.route
            ),
            TotalTickets AS (
                SELECT
                    CAST(COUNT(ticket_no) AS REAL) AS grand_total
                FROM
                    ticket_flights
            )
            SELECT
                rb.route,
                rb.total_tickets_sold,
                ROUND((rb.total_tickets_sold * 100.0 / tt.grand_total), 2) AS market_share_percent
            FROM
                RouteBookings AS rb
            CROSS JOIN
                TotalTickets AS tt
            WHERE
                rb.total_tickets_sold > 0
            ORDER BY
                market_share_percent ASC
            LIMIT 10;
        """)
        least_busy_routes = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        return jsonify({
            'top_occupancy_routes': top_occupancy_routes,
            'busiest_routes_market_share': busiest_routes_market_share,
            'least_busy_routes': least_busy_routes
        })
    except Exception as e:
        print(f"Error in passenger_demand: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/revenue-analysis')
def revenue_analysis():
    """Get revenue distribution metrics"""
    try:
        print("Loading revenue analysis data...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 6a. Revenue by Fare Class (aggregated across all routes)
        cursor.execute("""
            SELECT
                tf.fare_conditions,
                SUM(tf.amount) AS total_revenue_by_class,
                COUNT(*) as ticket_count,
                ROUND(AVG(tf.amount), 2) as avg_price
            FROM
                ticket_flights AS tf
            GROUP BY
                tf.fare_conditions
            ORDER BY
                total_revenue_by_class DESC;
        """)
        revenue_by_class = [dict(row) for row in cursor.fetchall()]
        
        # 6b. Top 3 Most Profitable Routes
        cursor.execute("""
            SELECT
                fr.route,
                SUM(tf.amount) AS total_revenue
            FROM
                flight_routes AS fr
            JOIN
                ticket_flights AS tf ON fr.flight_id = tf.flight_id
            GROUP BY
                fr.route
            ORDER BY
                total_revenue DESC
            LIMIT 3;
        """)
        top_revenue_routes = [dict(row) for row in cursor.fetchall()]
        
        # 6c. Top 3 Least Profitable Routes
        cursor.execute("""
            SELECT
                fr.route,
                SUM(tf.amount) AS total_revenue
            FROM
                flight_routes AS fr
            JOIN
                ticket_flights AS tf ON fr.flight_id = tf.flight_id
            GROUP BY
                fr.route
            HAVING
                total_revenue > 0
            ORDER BY
                total_revenue ASC
            LIMIT 3;
        """)
        least_revenue_routes = [dict(row) for row in cursor.fetchall()]
        
        # 7. Revenue by Fare Class & Route (Top 20)
        cursor.execute("""
            SELECT
                fr.route,
                tf.fare_conditions,
                SUM(tf.amount) AS total_revenue_by_class
            FROM
                flight_routes AS fr
            JOIN
                ticket_flights AS tf ON fr.flight_id = tf.flight_id
            GROUP BY
                fr.route, tf.fare_conditions
            ORDER BY
                total_revenue_by_class DESC
            LIMIT 20;
        """)
        revenue_by_class_route = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        return jsonify({
            'revenue_by_class': revenue_by_class,
            'top_revenue_routes': top_revenue_routes,
            'least_revenue_routes': least_revenue_routes,
            'revenue_by_class_route': revenue_by_class_route
        })
    except Exception as e:
        print(f"Error in revenue_analysis: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/resource-planning')
def resource_planning():
    """Get resource planning metrics (aircraft utilisation, turnaround)"""
    try:
        print("Loading resource planning data...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 8. Aircraft Type by Route (Top 20)
        cursor.execute("""
            SELECT
                fr.route,
                fr.aircraft_code,
                ad.model AS aircraft_model,
                COUNT(fr.flight_id) AS total_flights_on_route
            FROM
                flight_routes AS fr
            JOIN
                aircrafts_data AS ad ON fr.aircraft_code = ad.aircraft_code
            GROUP BY
                fr.route, fr.aircraft_code, ad.model
            ORDER BY
                total_flights_on_route DESC
            LIMIT 20;
        """)
        aircraft_by_route_raw = cursor.fetchall()
        
        aircraft_by_route = []
        for row in aircraft_by_route_raw:
            model = extract_json_value(row['aircraft_model'])
            aircraft_by_route.append({
                'route': row['route'],
                'aircraft_code': row['aircraft_code'],
                'aircraft_model': model,
                'total_flights_on_route': row['total_flights_on_route']
            })
        
        # 9. Top 3 Most Visited Destinations
        cursor.execute("""
            SELECT
                f.arrival_airport AS airport_code,
                ad.city AS destination_city,
                COUNT(f.flight_id) AS total_arrivals
            FROM
                flights AS f
            JOIN
                airports_data AS ad ON f.arrival_airport = ad.airport_code
            GROUP BY
                f.arrival_airport, destination_city
            ORDER BY
                total_arrivals DESC
            LIMIT 3;
        """)
        destinations_raw = cursor.fetchall()
        
        top_destinations = []
        for row in destinations_raw:
            city = extract_json_value(row['destination_city'])
            top_destinations.append({
                'airport_code': row['airport_code'],
                'city': city,
                'total_arrivals': row['total_arrivals']
            })
        
        # 10. Top 10 planes with most mileage
        cursor.execute("""
            SELECT
                f.aircraft_code,
                ad.model AS aircraft_model,
                SUM(ad.range) AS total_utilization_proxy_miles
            FROM
                flights AS f
            JOIN
                aircrafts_data AS ad ON f.aircraft_code = ad.aircraft_code
            WHERE
                f.status = 'Arrived'
            GROUP BY
                f.aircraft_code, aircraft_model
            ORDER BY
                total_utilization_proxy_miles DESC
            LIMIT 10;
        """)
        aircraft_utilization_raw = cursor.fetchall()
        
        aircraft_utilization = []
        for row in aircraft_utilization_raw:
            model = extract_json_value(row['aircraft_model'])
            aircraft_utilization.append({
                'aircraft_code': row['aircraft_code'],
                'aircraft_model': model,
                'total_mileage': row['total_utilization_proxy_miles']
            })
        
        # NEW: Get all aircraft codes with their models for the dropdown
        cursor.execute("""
            SELECT DISTINCT
                f.aircraft_code,
                ad.model AS aircraft_model,
                COUNT(f.flight_id) as flight_count
            FROM
                flights AS f
            JOIN
                aircrafts_data AS ad ON f.aircraft_code = ad.aircraft_code
            GROUP BY
                f.aircraft_code, ad.model
            ORDER BY
                flight_count DESC;
        """)
        aircraft_list_raw = cursor.fetchall()
        
        aircraft_list = []
        for row in aircraft_list_raw:
            model = extract_json_value(row['aircraft_model'])
            aircraft_list.append({
                'aircraft_code': row['aircraft_code'],
                'aircraft_model': model,
                'flight_count': row['flight_count']
            })
        
        # 11. Routes taken by SU9 (default)
        cursor.execute("""
            SELECT
                fr.flight_id,
                fr.route,
                SUBSTR(fr.scheduled_departure, 1, 16) AS scheduled_departure_time,
                fr.status
            FROM
                flight_routes AS fr
            WHERE
                fr.aircraft_code = 'SU9'
            ORDER BY
                fr.scheduled_departure ASC
            LIMIT 50;
        """)
        su9_routes = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        return jsonify({
            'aircraft_by_route': aircraft_by_route,
            'top_destinations': top_destinations,
            'aircraft_utilization': aircraft_utilization,
            'aircraft_list': aircraft_list,
            'su9_routes': su9_routes
        })
    except Exception as e:
        print(f"Error in resource_planning: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/aircraft-routes/<aircraft_code>')
def get_aircraft_routes(aircraft_code):
    """Get routes for a specific aircraft"""
    try:
        print(f"Loading routes for aircraft: {aircraft_code}")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT
                fr.flight_id,
                fr.route,
                SUBSTR(fr.scheduled_departure, 1, 16) AS scheduled_departure_time,
                fr.status
            FROM
                flight_routes AS fr
            WHERE
                fr.aircraft_code = ?
            ORDER BY
                fr.scheduled_departure ASC
            LIMIT 50;
        """, (aircraft_code,))
        
        routes = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        print(f"Found {len(routes)} routes for aircraft {aircraft_code}")
        return jsonify({'routes': routes})
        
    except Exception as e:
        print(f"Error in get_aircraft_routes: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

from bson import ObjectId

# CREATE booking
@app.route('/add_booking', methods=['GET', 'POST'])
def add_booking():
    if request.method == 'POST':
        booking = {
            'book_date': request.form['book_date'],
            'total_amount': int(request.form['total_amount']),
            'tickets': [
                {
                    'ticket_no': request.form['ticket_no'],
                    'passenger_id': request.form['passenger_id']
                    # Add other ticket fields here
                }
            ]
        }
        mongo.db.bookings.insert_one(booking)
        return render_template('add_booking.html', success=True)
    return render_template('add_booking.html', success=False)



# READ all bookings
@app.route('/api/nosql/bookings', methods=['GET'])
def get_nosql_bookings():
    bookings = mongo.db.bookings.find()
    output = []
    for b in bookings:
        b['_id'] = str(b['_id'])
        output.append(b)
    return jsonify(output)

# UPDATE booking
@app.route('/api/nosql/bookings/<booking_id>', methods=['PUT'])
def update_nosql_booking(booking_id):
    data = request.json
    update_fields = {key: value for key, value in data.items() if key != '_id'}
    result = mongo.db.bookings.update_one({'_id': ObjectId(booking_id)}, {'$set': update_fields})
    if result.matched_count:
        updated = mongo.db.bookings.find_one({'_id': ObjectId(booking_id)})
        updated['_id'] = str(updated['_id'])
        return jsonify(updated)
    return jsonify({'error': 'Booking not found'}), 404

# DELETE booking
@app.route('/api/nosql/bookings/<booking_id>', methods=['DELETE'])
def delete_nosql_booking(booking_id):
    result = mongo.db.bookings.delete_one({'_id': ObjectId(booking_id)})
    if result.deleted_count:
        return jsonify({'message': 'Booking deleted'})
    return jsonify({'error': 'Booking not found'}), 404


if __name__ == '__main__':
    # Initialize database views before starting the app
    try:
        init_db()
        print("\n" + "="*50)
        print("Starting Flask server...")
        print(f"Database location: {SQLITE_DB}")
        print("="*50 + "\n")
        app.run(debug=True, port=5000)
    except Exception as e:
        print(f"Failed to start application: {e}")
        import traceback
        traceback.print_exc()