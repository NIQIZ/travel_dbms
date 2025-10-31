import sqlite3
from pymongo import MongoClient
from datetime import datetime
import sys

sys.stdout.reconfigure(encoding='utf-8')

# Connect to SQLite
conn = sqlite3.connect('travel.sqlite')
cursor = conn.cursor()

# Connect to MongoDB
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['travel_nosql']

# Clear existing collections
mongo_db.bookings.drop()
mongo_db.flights.drop()
mongo_db.aircrafts.drop()
mongo_db.airports.drop()

print("=== MIGRATING TO MONGODB WITH DENORMALIZATION ===\n")

#############################################################################
# 1. DENORMALIZED BOOKINGS COLLECTION (Most Important)
#############################################################################
print("Creating denormalized bookings collection...")

cursor.execute("""
    SELECT 
        b.book_ref,
        b.book_date,
        b.total_amount,
        t.ticket_no,
        t.passenger_id,
        tf.flight_id,
        tf.fare_conditions,
        tf.amount as ticket_amount,
        f.flight_no,
        f.scheduled_departure,
        f.scheduled_arrival,
        f.departure_airport,
        f.arrival_airport,
        f.status,
        f.aircraft_code,
        bp.boarding_no,
        bp.seat_no
    FROM bookings b
    LEFT JOIN tickets t ON b.book_ref = t.book_ref
    LEFT JOIN ticket_flights tf ON t.ticket_no = tf.ticket_no
    LEFT JOIN flights f ON tf.flight_id = f.flight_id
    LEFT JOIN boarding_passes bp ON t.ticket_no = bp.ticket_no AND f.flight_id = bp.flight_id
    ORDER BY b.book_ref, t.ticket_no, tf.flight_id
""")

rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]

# Group by booking
bookings_dict = {}
for row in rows:
    data = dict(zip(columns, row))
    book_ref = data['book_ref']
    
    # Initialize booking document
    if book_ref not in bookings_dict:
        bookings_dict[book_ref] = {
            '_id': book_ref,
            'book_date': data['book_date'],
            'total_amount': data['total_amount'],
            'tickets': {}
        }
    
    # Add ticket information
    ticket_no = data['ticket_no']
    if ticket_no and ticket_no not in bookings_dict[book_ref]['tickets']:
        bookings_dict[book_ref]['tickets'][ticket_no] = {
            'ticket_no': ticket_no,
            'passenger_id': data['passenger_id'],
            'flights': []
        }
    
    # Add flight information to ticket
    if ticket_no and data['flight_id']:
        flight_info = {
            'flight_id': data['flight_id'],
            'flight_no': data['flight_no'],
            'fare_conditions': data['fare_conditions'],
            'amount': data['ticket_amount'],
            'scheduled_departure': data['scheduled_departure'],
            'scheduled_arrival': data['scheduled_arrival'],
            'departure_airport': data['departure_airport'],
            'arrival_airport': data['arrival_airport'],
            'status': data['status'],
            'aircraft_code': data['aircraft_code']
        }
        
        # Add boarding pass info if exists
        if data['boarding_no']:
            flight_info['boarding_pass'] = {
                'boarding_no': data['boarding_no'],
                'seat_no': data['seat_no']
            }
        
        bookings_dict[book_ref]['tickets'][ticket_no]['flights'].append(flight_info)

# Convert tickets dict to list and insert into MongoDB
for booking in bookings_dict.values():
    booking['tickets'] = list(booking['tickets'].values())

if bookings_dict:
    mongo_db.bookings.insert_many(list(bookings_dict.values()))
    print(f"✓ Inserted {len(bookings_dict)} denormalized bookings")

#############################################################################
# 2. FLIGHTS COLLECTION (with aircraft and airport details embedded)
#############################################################################
print("\nCreating denormalized flights collection...")

cursor.execute("""
    SELECT 
        f.flight_id,
        f.flight_no,
        f.scheduled_departure,
        f.scheduled_arrival,
        f.departure_airport,
        f.arrival_airport,
        f.status,
        f.aircraft_code,
        f.actual_departure,
        f.actual_arrival,
        ac.model as aircraft_model,
        ac.range as aircraft_range,
        dep.airport_name as dep_airport_name,
        dep.city as dep_city,
        dep.timezone as dep_timezone,
        arr.airport_name as arr_airport_name,
        arr.city as arr_city,
        arr.timezone as arr_timezone
    FROM flights f
    LEFT JOIN aircrafts_data ac ON f.aircraft_code = ac.aircraft_code
    LEFT JOIN airports_data dep ON f.departure_airport = dep.airport_code
    LEFT JOIN airports_data arr ON f.arrival_airport = arr.airport_code
""")

flights = []
for row in cursor.fetchall():
    flight_doc = {
        '_id': row[0],  # flight_id
        'flight_no': row[1],
        'scheduled_departure': row[2],
        'scheduled_arrival': row[3],
        'status': row[6],
        'actual_departure': row[8],
        'actual_arrival': row[9],
        'aircraft': {
            'code': row[7],
            'model': row[10],
            'range': row[11]
        },
        'departure': {
            'airport_code': row[4],
            'airport_name': row[12],
            'city': row[13],
            'timezone': row[14]
        },
        'arrival': {
            'airport_code': row[5],
            'airport_name': row[15],
            'city': row[16],
            'timezone': row[17]
        }
    }
    flights.append(flight_doc)

if flights:
    mongo_db.flights.insert_many(flights)
    print(f"✓ Inserted {len(flights)} denormalized flights")

#############################################################################
# 3. AIRCRAFTS COLLECTION (with seats embedded)
#############################################################################
print("\nCreating aircrafts collection with embedded seats...")

cursor.execute("""
    SELECT 
        ac.aircraft_code,
        ac.model,
        ac.range,
        s.seat_no,
        s.fare_conditions
    FROM aircrafts_data ac
    LEFT JOIN seats s ON ac.aircraft_code = s.aircraft_code
    ORDER BY ac.aircraft_code, s.seat_no
""")

aircrafts_dict = {}
for row in cursor.fetchall():
    code = row[0]
    if code not in aircrafts_dict:
        aircrafts_dict[code] = {
            '_id': code,
            'model': row[1],
            'range': row[2],
            'seats': []
        }
    
    if row[3]:  # if seat exists
        aircrafts_dict[code]['seats'].append({
            'seat_no': row[3],
            'fare_conditions': row[4]
        })

if aircrafts_dict:
    mongo_db.aircrafts.insert_many(list(aircrafts_dict.values()))
    print(f"✓ Inserted {len(aircrafts_dict)} aircrafts with embedded seats")

#############################################################################
# 4. AIRPORTS COLLECTION (standalone - reference data)
#############################################################################
print("\nCreating airports collection...")

cursor.execute("SELECT airport_code, airport_name, city, coordinates, timezone FROM airports_data")
airports = []
for row in cursor.fetchall():
    airports.append({
        '_id': row[0],
        'airport_name': row[1],
        'city': row[2],
        'coordinates': row[3],
        'timezone': row[4]
    })

if airports:
    mongo_db.airports.insert_many(airports)
    print(f"✓ Inserted {len(airports)} airports")

#############################################################################
# CREATE INDEXES FOR PERFORMANCE
#############################################################################
print("\nCreating indexes...")

# Bookings indexes
mongo_db.bookings.create_index('book_date')
mongo_db.bookings.create_index('tickets.passenger_id')
mongo_db.bookings.create_index('tickets.flights.flight_id')

# Flights indexes
mongo_db.flights.create_index('flight_no')
mongo_db.flights.create_index('scheduled_departure')
mongo_db.flights.create_index('departure.airport_code')
mongo_db.flights.create_index('arrival.airport_code')
mongo_db.flights.create_index('status')

# Aircrafts indexes
mongo_db.aircrafts.create_index('model')

# Airports indexes
mongo_db.airports.create_index('city')

print("✓ Indexes created")

#############################################################################
# VERIFICATION
#############################################################################
print("\n=== MIGRATION COMPLETE ===")
print(f"Collections created:")
print(f"  - bookings: {mongo_db.bookings.count_documents({})} documents")
print(f"  - flights: {mongo_db.flights.count_documents({})} documents")
print(f"  - aircrafts: {mongo_db.aircrafts.count_documents({})} documents")
print(f"  - airports: {mongo_db.airports.count_documents({})} documents")

# Show sample booking document
print("\n=== SAMPLE BOOKING DOCUMENT ===")
sample_booking = mongo_db.bookings.find_one()
if sample_booking:
    import json
    print(json.dumps(sample_booking, indent=2, default=str))

# Close connections
conn.close()
mongo_client.close()