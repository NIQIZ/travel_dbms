import sqlite3
import pandas as pd
import sys

# Ensure UTF-8 encoding for standard output
sys.stdout.reconfigure(encoding='utf-8')

#connect to the database
conn =sqlite3.connect('travel.sqlite')
c = conn.cursor()

#############################################################################################################################################################################################################
# Original Tables Before Cleaning
# print("Original Tables")
# print("=" * 50)
#############################################################################################################################################################################################################

# # Fetch and print the first 30 rows from aircrafts_data table
# c.execute("SELECT * FROM aircrafts_data LIMIT 0,30")
# rows = c.fetchall()
# if not rows:
#     print("No data found in aircrafts_data table.")
# else:
#     print("aircrafts_data table:")
#     for row in rows:
#         print(row)
# print("\n")

# # Fetch and print the first 30 rows from airports_data table
# c.execute("SELECT * FROM airports_data LIMIT 0,30")
# rows = c.fetchall()
# if not rows:
#     print("No data found in airports_data table.")
# else:
#     print("airports_data table:")
#     for row in rows:
#         print(row)
# print("\n")

# # Fetch and print the first 30 rows from boarding_passes table
# c.execute("SELECT * FROM boarding_passes LIMIT 0,30")
# rows = c.fetchall()
# if not rows:
#     print("No data found in boarding_passes table.")
# else:
#     print("boarding_passes table:")
#     for row in rows:
#         print(row)
# print("\n")

# Fetch and print the first 30 rows from bookings table
# c.execute("SELECT * FROM bookings LIMIT 0,30")
# rows = c.fetchall()
# if not rows:
#     print("No data found in bookings table.")
# else:
#     print("bookings table:")
#     for row in rows:
#         print(row)
# print("\n")

# # Fetch and print the first 30 rows from flights table
# c.execute("SELECT * FROM flights LIMIT 0,30")
# rows = c.fetchall()
# if not rows:
#     print("No data found in flights table.")
# else:
#     print("flights table:")
#     for row in rows:
#         print(row)
# print("\n")

# # Fetch and print the first 30 rows from seats table
# c.execute("SELECT * FROM seats LIMIT 0,30")
# rows = c.fetchall()
# if not rows:
#     print("No data found in seats table.")
# else:
#     print("seats table:")
#     for row in rows:
#         print(row)
# print("\n")

# # Fetch and print the first 30 rows from ticket_flights table
# c.execute("SELECT * FROM ticket_flights LIMIT 0,30")
# rows = c.fetchall()
# if not rows:
#     print("No data found in ticket_flights table.")
# else:
#     print("ticket_flights table:")
#     for row in rows:
#         print(row)
# print("\n")

# # Fetch and print the first 30 rows from tickets table
# c.execute("SELECT * FROM tickets LIMIT 0,30")
# rows = c.fetchall()
# if not rows:
#     print("No data found in tickets table.")
# else:
#     print("tickets table:")
#     for row in rows:
#         print(row)
# print("\n")


#############################################################################################################################################################################################################
# Cleaned Tables with correct Primary Keys, Foreign Keys, and Data Types
print("Cleaned Tables")
print("=" * 50)

#############################################################################################################################################################################################################
# Aircrafts Data Table
#############################################################################################################################################################################################################
c.execute("DROP TABLE IF EXISTS PK_aircrafts_data;")
c.execute("CREATE TABLE PK_aircrafts_data (aircraft_code varchar(10) PRIMARY KEY, model varchar(255), range INTEGER);")
c.execute("INSERT INTO PK_aircrafts_data (aircraft_code, model, range) SELECT CAST(aircraft_code AS varchar(10)), CAST(model->>'en' AS varchar(255)), CAST(range AS INTEGER) FROM aircrafts_data;")
c.execute("ALTER TABLE aircrafts_data RENAME TO aircrafts_data_old;")
c.execute("ALTER TABLE PK_aircrafts_data RENAME TO aircrafts_data;")
c.execute("DROP TABLE aircrafts_data_old;")

#############################################################################################################################################################################################################
# Airports Data Table
#############################################################################################################################################################################################################
c.execute("DROP TABLE IF EXISTS PK_airports_data;")
c.execute("CREATE TABLE PK_airports_data (airport_code varchar(10) PRIMARY KEY, airport_name varchar(255), city varchar(255), coordinates varchar(255), timezone varchar(50));")
c.execute("INSERT INTO PK_airports_data (airport_code, airport_name, city, coordinates, timezone) SELECT CAST(airport_code AS varchar(10)), CAST(airport_name->>'en' AS varchar(255)), CAST(city->>'en' AS varchar(255)), CAST(coordinates AS varchar(255)), CAST(timezone AS varchar(50)) FROM airports_data;")
c.execute("ALTER TABLE airports_data RENAME TO airports_data_old;")
c.execute("ALTER TABLE PK_airports_data RENAME TO airports_data;")
c.execute("DROP TABLE airports_data_old;")

#############################################################################################################################################################################################################
# Flights Table
#############################################################################################################################################################################################################
c.execute("DROP TABLE IF EXISTS PK_flights;")
c.execute("CREATE TABLE PK_flights (flight_id INTEGER PRIMARY KEY, flight_no varchar(10), scheduled_departure DATETIME, scheduled_arrival DATETIME, departure_airport varchar(10), arrival_airport varchar(10), status varchar(50), aircraft_code varchar(10), actual_departure DATETIME, actual_arrival DATETIME, FOREIGN KEY(departure_airport) REFERENCES airports_data(airport_code), FOREIGN KEY(arrival_airport) REFERENCES airports_data(airport_code), FOREIGN KEY(aircraft_code) REFERENCES aircrafts_data(aircraft_code));")
c.execute("INSERT INTO PK_flights (flight_id, flight_no, scheduled_departure, scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code, actual_departure, actual_arrival) SELECT CAST(flight_id AS INTEGER), CAST(flight_no AS varchar(10)), scheduled_departure, scheduled_arrival, CAST(departure_airport AS varchar(10)), CAST(arrival_airport AS varchar(10)), CAST(status AS varchar(50)), CAST(aircraft_code AS varchar(10)), actual_departure, actual_arrival FROM flights;")
c.execute("ALTER TABLE flights RENAME TO flights_old;")
c.execute("ALTER TABLE PK_flights RENAME TO flights;")
c.execute("DROP TABLE flights_old;")

#############################################################################################################################################################################################################
# Bookings Table
#############################################################################################################################################################################################################
c.execute("DROP TABLE IF EXISTS PK_bookings;")
c.execute("CREATE TABLE PK_bookings (book_ref varchar(10) PRIMARY KEY, book_date DATETIME, total_amount INTEGER);")
c.execute("INSERT INTO PK_bookings (book_ref, book_date, total_amount) SELECT CAST(book_ref AS varchar(10)), book_date, CAST(total_amount AS INTEGER) FROM bookings;")
c.execute("ALTER TABLE bookings RENAME TO bookings_old;")
c.execute("ALTER TABLE PK_bookings RENAME TO bookings;")
c.execute("DROP TABLE bookings_old;")

#############################################################################################################################################################################################################
# Tickets Table
#############################################################################################################################################################################################################
c.execute("DROP TABLE IF EXISTS PK_tickets;")
c.execute("CREATE TABLE PK_tickets (ticket_no INTEGER PRIMARY KEY, book_ref varchar(10), passenger_id varchar(20), FOREIGN KEY(book_ref) REFERENCES bookings(book_ref));")
c.execute("INSERT INTO PK_tickets (ticket_no, book_ref, passenger_id) SELECT CAST(ticket_no AS INTEGER), CAST(book_ref AS varchar(10)), CAST(passenger_id AS varchar(20)) FROM tickets;")
c.execute("ALTER TABLE tickets RENAME TO tickets_old;")
c.execute("ALTER TABLE PK_tickets RENAME TO tickets;")
c.execute("DROP TABLE tickets_old;")

#############################################################################################################################################################################################################
# Ticket Flights Table
#############################################################################################################################################################################################################
c.execute("DROP TABLE IF EXISTS PK_ticket_flights;")
c.execute("CREATE TABLE PK_ticket_flights (ticket_no INTEGER, flight_id INTEGER, fare_conditions varchar(50), amount INTEGER, PRIMARY KEY(ticket_no, flight_id), FOREIGN KEY(ticket_no) REFERENCES tickets(ticket_no), FOREIGN KEY(flight_id) REFERENCES flights(flight_id));")
c.execute("INSERT INTO PK_ticket_flights (ticket_no, flight_id, fare_conditions, amount) SELECT CAST(ticket_no AS INTEGER), CAST(flight_id AS INTEGER), CAST(fare_conditions AS varchar(50)), CAST(amount AS INTEGER) FROM ticket_flights;")
c.execute("ALTER TABLE ticket_flights RENAME TO ticket_flights_old;")
c.execute("ALTER TABLE PK_ticket_flights RENAME TO ticket_flights;")
c.execute("DROP TABLE ticket_flights_old;")

#############################################################################################################################################################################################################
# Seats Table
#############################################################################################################################################################################################################
c.execute("DROP TABLE IF EXISTS PK_seats;")
c.execute("CREATE TABLE PK_seats (aircraft_code varchar(10), seat_no varchar(10), fare_conditions varchar(50), PRIMARY KEY(aircraft_code, seat_no), FOREIGN KEY(aircraft_code) REFERENCES aircrafts_data(aircraft_code));")
c.execute("INSERT INTO PK_seats (aircraft_code, seat_no, fare_conditions) SELECT CAST(aircraft_code AS varchar(10)), CAST(seat_no AS varchar(10)), CAST(fare_conditions AS varchar(50)) FROM seats;")
c.execute("ALTER TABLE seats RENAME TO seats_old;")
c.execute("ALTER TABLE PK_seats RENAME TO seats;")
c.execute("DROP TABLE seats_old;")

#############################################################################################################################################################################################################
# Boarding Passes Table
#############################################################################################################################################################################################################
c.execute("DROP TABLE IF EXISTS PK_boarding_passes;")
c.execute("CREATE TABLE PK_boarding_passes (ticket_no INTEGER, flight_id INTEGER, boarding_no INTEGER, seat_no varchar(10), PRIMARY KEY(ticket_no, flight_id), FOREIGN KEY(ticket_no) REFERENCES tickets(ticket_no), FOREIGN KEY(flight_id) REFERENCES flights(flight_id));")
c.execute("INSERT INTO PK_boarding_passes (ticket_no, flight_id, boarding_no, seat_no) SELECT CAST(ticket_no AS INTEGER), CAST(flight_id AS INTEGER), CAST(boarding_no AS INTEGER), CAST(seat_no AS varchar(10)) FROM boarding_passes;")
c.execute("ALTER TABLE boarding_passes RENAME TO boarding_passes_old;")
c.execute("ALTER TABLE PK_boarding_passes RENAME TO boarding_passes;")
c.execute("DROP TABLE boarding_passes_old;")


#list all tables in the database
c.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = c.fetchall()
if not tables:
    print("No tables found in the database.")
else:
    print("Tables")
    print("=" * 20)
    for table in tables:
        print(table[0])
print("\n")

# AIRCRAFTS DATA TABLE DETAILS==============================================================================================================================================================================
# Fetch and print the first 30 rows from aircrafts_data table
c.execute("SELECT * FROM aircrafts_data LIMIT 0,30")
rows = c.fetchall()
if not rows:
    print("No data found in aircrafts_data table.")
else:
    print("aircrafts_data table:")
    c.execute("PRAGMA table_info(aircrafts_data);")
    cols = c.fetchall()
    title = [col[1] for col in cols]
    print(title)
    for row in rows:
        print(row)
print("\n")

#Primary Key information for aircrafts_data table
c.execute("PRAGMA table_info(aircrafts_data)")
cols = c.fetchall()
pk = sorted([(col[5], col[1]) for col in cols if col[5] > 0])
if pk:
    print("Primary key column(s):", [name for _, name in pk])
else:
    print("No PRIMARY KEY on this table")

# Foreign Key information for aircrafts_data table
c.execute("PRAGMA foreign_key_list(aircrafts_data)")
fks = c.fetchall()
if not fks:
    print("No foreign keys found in aircrafts_data table.")
else:
    print("Foreign key(s) in aircrafts_data table:")
    for fk in fks:
        print(f"table='{fk[2]}', from='{fk[3]}', to='{fk[4]}', on_update='{fk[5]}', on_delete='{fk[6]}', match='{fk[7]}'")
print("\n")

# AIRPORTS DATA TABLE DETAILS==============================================================================================================================================================================
# Fetch and print the first 30 rows from airports_data table
c.execute("SELECT * FROM airports_data LIMIT 0,30")
rows = c.fetchall()
if not rows:
    print("No data found in airports_data table.")
else:
    print("airports_data table:")
    c.execute("PRAGMA table_info(airports_data);")
    cols = c.fetchall()
    title = [col[1] for col in cols]
    print(title)
    for row in rows:
        print(row)
print("\n")

#Primary Key information for airports_data table
c.execute("PRAGMA table_info(airports_data)")
cols = c.fetchall()
pk = sorted([(col[5], col[1]) for col in cols if col[5] > 0])
if pk:
    print("Primary key column(s):", [name for _, name in pk])
else:
    print("No PRIMARY KEY on this table")

# Foreign Key information for airports_data table
c.execute("PRAGMA foreign_key_list(airports_data)")
fks = c.fetchall()
if not fks:
    print("No foreign keys found in airports_data table.")
else:
    print("Foreign key(s) in airports_data table:")
    for fk in fks:
        print(f"table='{fk[2]}', from='{fk[3]}', to='{fk[4]}', on_update='{fk[5]}', on_delete='{fk[6]}', match='{fk[7]}'")
print("\n") 

# BOARDING PASSES TABLE DETAILS==============================================================================================================================================================================
# Fetch and print the first 30 rows from boarding_passes table
c.execute("SELECT * FROM boarding_passes LIMIT 0,30")
rows = c.fetchall()
if not rows:
    print("No data found in boarding_passes table.")
else:
    print("boarding_passes table:")
    c.execute("PRAGMA table_info(boarding_passes);")
    cols = c.fetchall()
    title = [col[1] for col in cols]
    print(title)
    for row in rows:
        print(row)
print("\n")

#Primary Key information for boarding_passes table
c.execute("PRAGMA table_info(boarding_passes)")
cols = c.fetchall()
pk = sorted([(col[5], col[1]) for col in cols if col[5] > 0])
if pk:
    print("Primary key column(s):", [name for _, name in pk])
else:
    print("No PRIMARY KEY on this table")

# Foreign Key information for boarding_passes table
c.execute("PRAGMA foreign_key_list(boarding_passes)")
fks = c.fetchall()
if not fks:
    print("No foreign keys found in boarding_passes table.")
else:
    print("Foreign key(s) in boarding_passes table:")
    for fk in fks:
        print(f"table='{fk[2]}', from='{fk[3]}', to='{fk[4]}', on_update='{fk[5]}', on_delete='{fk[6]}', match='{fk[7]}'")
print("\n") 

# BOOKINGS TABLE DETAILS==============================================================================================================================================================================
# Fetch and print the first 30 rows from bookings table
c.execute("SELECT * FROM bookings LIMIT 0,30")
rows = c.fetchall()
if not rows:
    print("No data found in bookings table.")
else:
    print("bookings table:")
    c.execute("PRAGMA table_info(bookings);")
    cols = c.fetchall()
    title = [col[1] for col in cols]
    print(title)
    for row in rows:
        print(row)
print("\n")

#Primary Key information for bookings table
c.execute("PRAGMA table_info(bookings)")
cols = c.fetchall()
pk = sorted([(col[5], col[1]) for col in cols if col[5] > 0])
if pk:
    print("Primary key column(s):", [name for _, name in pk])
else:
    print("No PRIMARY KEY on this table")

# Foreign Key information for bookings table
c.execute("PRAGMA foreign_key_list(bookings)")
fks = c.fetchall()
if not fks:
    print("No foreign keys found in bookings table.")
else:
    print("Foreign key(s) in bookings table:")
    for fk in fks:
        print(f"table='{fk[2]}', from='{fk[3]}', to='{fk[4]}', on_update='{fk[5]}', on_delete='{fk[6]}', match='{fk[7]}'")
print("\n") 

#Flights TABLE DETAILS==============================================================================================================================================================================
# Fetch and print the first 30 rows from flights table
c.execute("SELECT * FROM flights LIMIT 0,30")
rows = c.fetchall()
if not rows:
    print("No data found in flights table.")
else:
    print("flights table:")
    c.execute("PRAGMA table_info(flights);")
    cols = c.fetchall()
    title = [col[1] for col in cols]
    print(title)
    for row in rows:
        print(row)
print("\n")

#Primary Key information for flights table
c.execute("PRAGMA table_info(flights)")
cols = c.fetchall()
pk = sorted([(col[5], col[1]) for col in cols if col[5] > 0])
if pk:
    print("Primary key column(s):", [name for _, name in pk])
else:
    print("No PRIMARY KEY on this table")

# Foreign Key information for flights table
c.execute("PRAGMA foreign_key_list(flights)")
fks = c.fetchall()
if not fks:
    print("No foreign keys found in flights table.")
else:
    print("Foreign key(s) in flights table:")
    for fk in fks:
        print(f"table='{fk[2]}', from='{fk[3]}', to='{fk[4]}', on_update='{fk[5]}', on_delete='{fk[6]}', match='{fk[7]}'")
print("\n")

# SEATS TABLE DETAILS==============================================================================================================================================================================
# Fetch and print the first 30 rows from seats table
c.execute("SELECT * FROM seats LIMIT 0,30")
rows = c.fetchall()
if not rows:  
    print("No data found in seats table.")
else:
    print("seats table:")
    c.execute("PRAGMA table_info(seats);")
    cols = c.fetchall()
    title = [col[1] for col in cols]
    print(title)
    for row in rows:
        print(row)
print("\n")

#Primary Key information for seats table
c.execute("PRAGMA table_info(seats)")
cols = c.fetchall()
pk = sorted([(col[5], col[1]) for col in cols if col[5] > 0])
if pk:
    print("Primary key column(s):", [name for _, name in pk])
else:
    print("No PRIMARY KEY on this table")

# Foreign Key information for seats table
c.execute("PRAGMA foreign_key_list(seats)")
fks = c.fetchall()
if not fks:
    print("No foreign keys found in seats table.")
else:
    print("Foreign key(s) in seats table:")
    for fk in fks:
        print(f"table='{fk[2]}', from='{fk[3]}', to='{fk[4]}', on_update='{fk[5]}', on_delete='{fk[6]}', match='{fk[7]}'")
print("\n")

# TICKET FLIGHTS TABLE DETAILS==============================================================================================================================================================================
# Fetch and print the first 30 rows from ticket_flights table
c.execute("SELECT * FROM ticket_flights LIMIT 0,30")
rows = c.fetchall()
if not rows:
    print("No data found in ticket_flights table.")
else:
    print("ticket_flights table:")
    c.execute("PRAGMA table_info(ticket_flights);")
    cols = c.fetchall()
    title = [col[1] for col in cols]
    print(title)
    for row in rows:
        print(row)
print("\n")

#Primary Key information for ticket_flights table
c.execute("PRAGMA table_info(ticket_flights)")
cols = c.fetchall()
pk = sorted([(col[5], col[1]) for col in cols if col[5] > 0])
if pk:
    print("Primary key column(s):", [name for _, name in pk])
else:
    print("No PRIMARY KEY on this table")

# Foreign Key information for ticket_flights table
c.execute("PRAGMA foreign_key_list(ticket_flights)")
fks = c.fetchall()
if not fks:
    print("No foreign keys found in ticket_flights table.")
else:
    print("Foreign key(s) in ticket_flights table:")
    for fk in fks:
        print(f"table='{fk[2]}', from='{fk[3]}', to='{fk[4]}', on_update='{fk[5]}', on_delete='{fk[6]}', match='{fk[7]}'")
print("\n")

# TICKETS TABLE DETAILS==============================================================================================================================================================================
# Fetch and print the first 30 rows from tickets table
c.execute("SELECT * FROM tickets LIMIT 0,30")
rows = c.fetchall()
if not rows:
    print("No data found in tickets table.")
else:
    print("tickets table:")
    c.execute("PRAGMA table_info(tickets);")
    cols = c.fetchall()
    title = [col[1] for col in cols]
    print(title)
    for row in rows:
        print(row)
print("\n")

#Primary Key information for tickets table
c.execute("PRAGMA table_info(tickets)")
cols = c.fetchall()
pk = sorted([(col[5], col[1]) for col in cols if col[5] > 0])
if pk:
    print("Primary key column(s):", [name for _, name in pk])
else:
    print("No PRIMARY KEY on this table")

# Foreign Key information for tickets table
c.execute("PRAGMA foreign_key_list(tickets)")
fks = c.fetchall()
if not fks:
    print("No foreign keys found in tickets table.")
else:
    print("Foreign key(s) in tickets table:")
    for fk in fks:
        print(f"table='{fk[2]}', from='{fk[3]}', to='{fk[4]}', on_update='{fk[5]}', on_delete='{fk[6]}', match='{fk[7]}'")
print("\n")
