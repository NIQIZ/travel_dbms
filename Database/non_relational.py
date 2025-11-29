import sqlite3
from pymongo import MongoClient
import sys
import json

# Ensure UTF-8 encoding
sys.stdout.reconfigure(encoding='utf-8')

# Connect to SQLite
conn = sqlite3.connect('travel.sqlite')
conn.row_factory = sqlite3.Row # Access columns by name
cursor = conn.cursor()

# Connect to MongoDB
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['travel_nosql']

# Clear existing collections
print("Cleaning old collections...")
mongo_db.bookings.drop()
mongo_db.flights.drop()
mongo_db.aircrafts.drop()
mongo_db.airports.drop()

print("=== STARTING OPTIMIZED MIGRATION ===\n")

# Helper to parse JSON strings from SQLite
def parse_json(data):
    try:
        if not data: return "Unknown"
        loaded = json.loads(data)
        if isinstance(loaded, dict) and 'en' in loaded:
            return loaded['en']
        return str(loaded)
    except:
        return str(data)

#############################################################################
# 1. AIRCRAFTS (With Embedded Seats)
#############################################################################
print("1. Migrating Aircrafts (Embedding Seats)...")

# Fetch all seats first to organize by aircraft
cursor.execute("SELECT aircraft_code, seat_no, fare_conditions FROM seats")
seats_map = {}
for row in cursor.fetchall():
    code = row['aircraft_code']
    if code not in seats_map: seats_map[code] = []
    seats_map[code].append({
        'seat_no': row['seat_no'],
        'fare_conditions': row['fare_conditions']
    })

cursor.execute("SELECT * FROM aircrafts_data")
aircrafts_list = []
for row in cursor.fetchall():
    code = row['aircraft_code']
    aircrafts_list.append({
        '_id': code, # Using code as ID is cleaner for lookups
        'model': parse_json(row['model']),
        'range': row['range'],
        'seats': seats_map.get(code, []), # Embed the seats here
        'version': 1
    })

if aircrafts_list:
    mongo_db.aircrafts.insert_many(aircrafts_list)
    print(f"   -> Inserted {len(aircrafts_list)} aircraft documents")

#############################################################################
# 2. FLIGHTS (The Operational Core)
#############################################################################
print("2. Migrating Flights (Embedding Airport & Aircraft Data)...")

cursor.execute("""
    SELECT 
        f.*,
        ac.model, ac.range,
        dep.airport_name as dep_name, dep.city as dep_city, dep.timezone as dep_tz,
        arr.airport_name as arr_name, arr.city as arr_city, arr.timezone as arr_tz
    FROM flights f
    LEFT JOIN aircrafts_data ac ON f.aircraft_code = ac.aircraft_code
    LEFT JOIN airports_data dep ON f.departure_airport = dep.airport_code
    LEFT JOIN airports_data arr ON f.arrival_airport = arr.airport_code
""")

flights_list = []
for row in cursor.fetchall():
    flights_list.append({
        '_id': row['flight_id'], # Keep integer ID for easy referencing
        'flight_no': row['flight_no'],
        'scheduled_departure': row['scheduled_departure'],
        'scheduled_arrival': row['scheduled_arrival'],
        'status': row['status'],
        'actual_departure': row['actual_departure'],
        'actual_arrival': row['actual_arrival'],
        'aircraft': {
            'code': row['aircraft_code'],
            'model': parse_json(row['model'])
        },
        'departure': {
            'airport_code': row['departure_airport'],
            'airport_name': parse_json(row['dep_name']),
            'city': parse_json(row['dep_city']),
            'timezone': row['dep_tz']
        },
        'arrival': {
            'airport_code': row['arrival_airport'],
            'airport_name': parse_json(row['arr_name']),
            'city': parse_json(row['arr_city']),
            'timezone': row['arr_tz']
        },
        'version': 1
    })

if flights_list:
    mongo_db.flights.insert_many(flights_list)
    print(f"   -> Inserted {len(flights_list)} flight documents")

#############################################################################
# 3. BOOKINGS (The Transactional View)
#############################################################################
print("3. Migrating Bookings (Embedding Tickets, Referencing Flights)...")

cursor.execute("""
    SELECT 
        b.book_ref, b.book_date, b.total_amount,
        t.ticket_no, t.passenger_id,
        tf.flight_id, tf.fare_conditions, tf.amount as ticket_amount,
        f.flight_no, f.departure_airport, f.arrival_airport, f.scheduled_departure,
        bp.boarding_no, bp.seat_no
    FROM bookings b
    JOIN tickets t ON b.book_ref = t.book_ref
    JOIN ticket_flights tf ON t.ticket_no = tf.ticket_no
    JOIN flights f ON tf.flight_id = f.flight_id
    LEFT JOIN boarding_passes bp ON t.ticket_no = bp.ticket_no AND tf.flight_id = bp.flight_id
    ORDER BY b.book_ref
""")

bookings_map = {}

# Process row by row
for row in cursor.fetchall():
    book_ref = row['book_ref']
    
    if book_ref not in bookings_map:
        bookings_map[book_ref] = {
            '_id': book_ref,
            'book_date': row['book_date'],
            'total_amount': row['total_amount'],
            'tickets': {},
            'version': 1
        }
    
    ticket_no = row['ticket_no']
    if ticket_no not in bookings_map[book_ref]['tickets']:
        bookings_map[book_ref]['tickets'][ticket_no] = {
            'ticket_no': ticket_no,
            'passenger_id': row['passenger_id'], 
            'contact_data': "{}",
            'flight_legs': []
        }
        
    leg = {
        'flight_id': row['flight_id'],
        'flight_no': row['flight_no'],
        'route': f"{row['departure_airport']} -> {row['arrival_airport']}",
        'scheduled_departure': row['scheduled_departure'], 
        'fare_conditions': row['fare_conditions'],
        'amount': row['ticket_amount']
    }
    
    if row['boarding_no']:
        leg['boarding_pass'] = {
            'boarding_no': row['boarding_no'],
            'seat_no': row['seat_no']
        }
        
    bookings_map[book_ref]['tickets'][ticket_no]['flight_legs'].append(leg)

final_bookings = []
for b in bookings_map.values():
    b['tickets'] = list(b['tickets'].values())
    final_bookings.append(b)

if final_bookings:
    mongo_db.bookings.insert_many(final_bookings)
    print(f"   -> Inserted {len(final_bookings)} booking documents")

#############################################################################
# 4. AIRPORTS (Reference Data)
#############################################################################
print("4. Migrating Airports...")
cursor.execute("SELECT * FROM airports_data")
airports = []
for row in cursor.fetchall():
    airports.append({
        '_id': row['airport_code'],
        'airport_name': parse_json(row['airport_name']),
        'city': parse_json(row['city']),
        'coordinates': row['coordinates'],
        'timezone': row['timezone'],
        'version': 1
    })
if airports:
    mongo_db.airports.insert_many(airports)
    print(f"   -> Inserted {len(airports)} airports")

print("\n=== MIGRATION COMPLETE ===")