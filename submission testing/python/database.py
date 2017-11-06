#!/usr/bin/env python3

from modules import pg8000
import configparser
import json
from flask import Flask
from flask.ext.bcrypt import Bcrypt

# TODO
# Change cursor and connection close to use a with block

#####################################################
##  Database Connect
#####################################################

'''
Connects to the database using the connection string
'''
def database_connect():
    # Read the config file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Create a connection to the database
    connection = None
    try:
        # Parses the config file and connects using the connect string
        connection = pg8000.connect(database=config['DATABASE']['user'],
                                    user=config['DATABASE']['user'],
                                    password=config['DATABASE']['password'],
                                    host=config['DATABASE']['host'])
    except pg8000.OperationalError as e:
        print("""Error, you haven't updated your config.ini or you have a bad
        connection, please try again. (Update your files first, then check
        internet connection)
        """)
        print(e)
    # return the connection to use
    return connection

#####################################################
##  Login
#####################################################

'''
Check that the users information exists in the database.

- True = return the user data
- False = return None
'''
def check_login(member_id, password):
    try:
        # Create Database Connection
        conn=database_connect()
        cursor = conn.cursor()
        # Find the users data
        cursor.execute("""SELECT member_id, title, given_names, family_name, country_name, place_name,
            (CASE
                WHEN member_id IN (Select * from athlete) THEN 'athlete'
                WHEN member_id IN (Select * from staff) THEN 'staff'
                WHEN member_id IN (Select * from official) THEN 'official'
                ELSE 'No category'
            END) AS user_type, pass_word
            FROM member
            INNER JOIN country USING (country_code)
            INNER JOIN place ON member.accommodation = place.place_id
            WHERE member_id=%s
            LIMIT 1""", (member_id,))
        user_data=cursor.fetchone() # Retrieve Query result
    finally:
        cursor.close() # Close Curosr
        conn.close()

    bc = Bcrypt(None)
    if user_data is None: # INVALID USERNAME
        return ()

    # Password check
    # people with empty passwords don't get any hahsing
    # Note: it is currently impossible to enter an empty password with the current form so these users cannot login.
    if (are_passwords_hashed()):
        if not (password=='' and user_data[7]==''):
            password_hashed=bytes(user_data[7], 'utf8')
            if not bc.check_password_hash(password_hashed,password): # INVALID PASSWORD
                return ()
    else: #compare unhashed passwords
        if (password!=user_data[7]):
            return ()

    tuples = {
            'member_id': user_data[0],
            'title': user_data[1],
            'first_name': user_data[2],
            'family_name': user_data[3],
            'country_name': user_data[4],
            'residence': user_data[5],
            'member_type': user_data[6]
        }
    return tuples


#####################################################
## Member Information
#####################################################

'''
Get the details for a member, including:
    - all the accommodation details,
    - information about their events
    - medals
    - bookings.

If they are an official, then they will have no medals, just a list of their roles.
'''
def member_details(member_id, mem_type):
    try:
        # Create Database Connection
        conn= database_connect()
        cursor = conn.cursor()

        cursor.execute("""SELECT place_name, address, gps_lat, gps_long
            FROM member
            INNER JOIN place ON member.accommodation = place.place_id
            WHERE member_id=%s""", (member_id,))
        accom_rows=cursor.fetchone()

        accommodation_details = {
            'name': accom_rows[0],
            'address': accom_rows[1],
            'gps_lat': accom_rows[2],
            'gps_lon': accom_rows[3]
        }

        cursor.execute("""SELECT count(*)
            FROM booking
            WHERE booked_for=%s""", (member_id,))
        num_bookings=cursor.fetchone()[0]

        # Check what type of member we are
        if(mem_type == 'athlete'):
            cursor.execute("""
                SELECT SUM(CASE WHEN NOT medal IS NULL THEN 1 ELSE 0 END) AS total,
                SUM(CASE WHEN medal='G' THEN 1 ELSE 0 END) AS gold,
                SUM(CASE WHEN medal='S' THEN 1 ELSE 0 END) AS silver,
                SUM(CASE WHEN medal='B' THEN 1 ELSE 0 END) AS bronze
                FROM participates
                WHERE athlete_id=%s""", (member_id,))
            medal_counts=cursor.fetchone()

            member_information = {
                'total_events': medal_counts[0],
                'gold': medal_counts[1],
                'silver': medal_counts[2],
                'bronze': medal_counts[3],
                'bookings': num_bookings
            }
        elif(mem_type == 'official'):
            cursor.execute("""
                SELECT role, count(*)
                FROM runsevent
                WHERE member_id = %s
                GROUP BY role
                HAVING COUNT(*) >= ALL (SELECT COUNT(*) FROM runsevent WHERE member_id = %s GROUP BY role)
                """, (member_id,member_id,))
            if cursor.rowcount == 0:
                role_count=['None',0]
            else:
                role_count=cursor.fetchone()

            member_information = {
                'favourite_role' : role_count[0],
                'total_events' : role_count[1],
                'bookings': num_bookings
            }
        else:
            member_information = {'bookings': num_bookings}
    finally:
        cursor.close() # Close Curosr
        conn.close()
    return {'accommodation': accommodation_details, 'member_details': member_information}

#####################################################
##  Booking (make, get all, get details)
#####################################################

'''
Make a booking for a member.
Only a staff type member should be able to do this ;)
Note: `my_member_id` = Staff ID (bookedby)
      `for_member` = member id that you are booking for
'''
def make_booking(my_member_id, for_member, vehicle, date, hour, start_destination, end_destination):
    try:
        print('trying to make booking')
        # Create Database Connection
        return_val=False
        conn=database_connect()
        cursor = conn.cursor()
        # Valid my_member_id
        cursor.execute("""
            SELECT *
            FROM staff
            WHERE member_id = %s """, (my_member_id,))
        if (cursor.rowcount==0):
            # Person making bookings is not staff
            print('Person making bookings is not staff')
            return False

        # Valid for_member
        cursor.execute("""
            SELECT *
            FROM member
            WHERE member_id = %s """, (for_member,))
        if (cursor.rowcount==0):
            # Invalid person booked for
            print('Invalid person booked for')
            return False
                
        if len(hour)==1:
            full_timestamp=date+' 0'+hour+':00:00'
        else:
            full_timestamp=date+' '+hour+':00:00'
        
        #Does the journey exist?
        print('checking for journey')
        cursor.execute("""
            SELECT journey_id, nbooked, from_place, to_place
            FROM journey
            WHERE vehicle_code = %s AND depart_time = %s AND from_place = %s and to_place = %s""", (vehicle,full_timestamp,start_destination,end_destination,))
        journey_info=cursor.fetchone()

        #Check if the journey exists
        if (cursor.rowcount==0):
            print('no journey')
            return False
        print('journey exists')
        #What is the vehicle capacity
        cursor.execute("""
            SELECT capacity
            FROM vehicle
            WHERE vehicle_code = %s""", (vehicle,))
        vehicle_info=cursor.fetchone()

        #Valid Vehicle Check
        if vehicle_info is None:
            print('invalid vehicle_code input')
            return False

        # Vehicle capacity check
        if journey_info[1]>=vehicle_info[0]:
            print('journey %s, capacity %s' % (journey_info[1],vehicle_info[0]))
            print('vehicle does not have the capacity')
            return False
        else:
            new_nbooked=journey_info[1]+1

        print('Updating nbooked')
        # update nbooked for the journey
        cursor.execute(""" UPDATE journey
            SET nbooked = %s
            WHERE journey_id = %s""",
        (str(new_nbooked),journey_info[0],))

        print('creating booking')
        # create the new booking
        cursor.execute(""" INSERT INTO booking
        VALUES (%s,%s,CURRENT_TIMESTAMP,%s) """,
        (my_member_id,for_member,journey_info[0],))

        return_val=True
        conn.commit()
        return True
    finally:
        print('closing connection')
        cursor.close() # Close Cursor
        conn.close()

'''
List all the bookings for a member
'''
def all_bookings(member_id):
    try:
        # Create Database Connection
        conn=database_connect()
        cursor = conn.cursor()

        # Find the users data
        cursor.execute("""
            SELECT vehicle_code, CAST(depart_time AS DATE), CAST(depart_time AS TIME),
            p_to.place_name, p_from.place_name
            FROM booking
            INNER JOIN journey USING (journey_id)
            INNER JOIN place p_to ON p_to.place_id = journey.to_place
            INNER JOIN place p_from ON p_from.place_id = journey.from_place
            WHERE booked_for = %s """, (member_id,))

        bookings_db=cursor.fetchall() # Retrieve Query result (Fetchall returns array of arrays)

        bookings = [{
            'vehicle': row[0],
            'start_day': row[1],
            'start_time': row[2],
            'to': row[3],
            'from': row[4]
        } for row in bookings_db]
    finally:
        cursor.close() # Close Curosr
        conn.close()
    return bookings

'''
List all the bookings for a member on a certain day
'''
def day_bookings(member_id, day):
    try:
        # Create Database Connection
        conn=database_connect()
        cursor = conn.cursor()

        print(day)
        # Find the users data
        cursor.execute("""
            SELECT vehicle_code, CAST(depart_time AS DATE), CAST(depart_time AS TIME),
            p_to.place_name, p_from.place_name
            FROM booking
            INNER JOIN journey USING (journey_id)
            INNER JOIN place p_to ON p_to.place_id = journey.to_place
            INNER JOIN place p_from ON p_from.place_id = journey.from_place
            WHERE booked_for = %s AND date(depart_time) = %s """, (member_id,day,))

        bookings_db=cursor.fetchall() # Retrieve Query result (Fetchall returns array of arrays)

        bookings = [{
            'vehicle': row[0],
            'start_day': row[1],
            'start_time': row[2],
            'to': row[3],
            'from': row[4]
        } for row in bookings_db]
    finally:
        cursor.close()
        conn.close()
    return bookings


'''
Get the booking information for a specific booking
'''
def get_booking(b_date, b_hour, vehicle, from_place, to_place):
    try:
        conn=database_connect()
        cursor = conn.cursor()

        # Find the users data
        full_timestamp=create_full_timestamp(b_date,b_hour)
        cursor.execute("""SELECT booked_by, when_booked
            FROM journey
            INNER JOIN booking USING (journey_id)
            WHERE vehicle_code = %s AND
                depart_time = %s """, (vehicle,full_timestamp,))

        booking_info=cursor.fetchone() # Retrieve Query result
        print(booking_info)
    finally:
        cursor.close() # Close Curosr
        conn.close()

        if booking_info is None:
            return None

        booking = {
        'vehicle': vehicle,
        'start_day': b_date,
        'start_time': b_hour,
        'to': to_place,
        'from': from_place,
        'booked_by': booking_info[0],
        'whenbooked': booking_info[1]
        }

    return booking

#####################################################
## Journeys
#####################################################

'''
List all the journeys between two places.
'''
def all_journeys(from_place, to_place):

    conn=database_connect()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT vehicle_code, CAST(depart_time AS DATE), CAST(depart_time AS TIME), to_place, from_place
        FROM journey
        WHERE from_place = %s AND to_place = %s """, (from_place,to_place,))

    journeys_db = cursor.fetchall()

    # This is the example data, it will appaer if our query is not working
    journeys_db = [
    ['TR470R', '21/12/2020', '0600', 'SIT', 'Wentworth', 7, 8]
    ]

    journeys = [{
    'vehicle': row[0],
    'start_day': row[1],
    'start_time': row[2],
    'to' : row[3],
    'from' : row[4]
    } for row in journeys_db]

    return journeys

'''
Get all of the journeys for a given day, from and to a selected place.
'''
def get_day_journeys(from_place, to_place, journey_date):

    # TODO - update the journeys_db variable to get information from the database about this journey!
    # List all the journeys between two locations.
    # Should be chronologically ordered
    # It is a list of lists

    # Format:
    # [
    #   [ vehicle, day, time, to, from, nbooked, vehicle_capacity],
    #   ...
    # ]

    conn=database_connect()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT vehicle_code, CAST(depart_time AS DATE), CAST(depart_time AS TIME), to_place, from_place
        FROM journey
        WHERE from_place = %s AND journey_date = %s AND to_place = %s
        ORDER BY depart_time """, (from_place,journey_date, to_place,))


    journeys_db = cursor.fetchall()

    journeys_db = [
        ['TR470R', '21/12/2020', '0600', 'SIT', 'Wentworth', 7, 8]
    ]

    journeys = [{
        'vehicle': row[0],
        'start_day': row[1],
        'start_time': row[2],
        'to': row[3],
        'from': row[4]
    } for row in journeys_db]

    return journeys



#####################################################
## Events
#####################################################

'''
List all the events running in the olympics
'''
def all_events():
    try:
        conn=database_connect()
        cursor = conn.cursor()

        cursor.execute("""SELECT event.event_name, event.event_start, sport.sport_name,  event.sport_venue
            FROM Sport INNER JOIN event ON (event.sport_id = Sport.sport_id )
            ORDER BY event.event_start""")

        event_db = cursor.fetchall()
    finally:
        # cursor.close()
        # conn.close()
        print('error')

    events_db = [
        ['200M Freestyle', '0800', 'Swimming', 'Olympic Swimming Pools'],
        ['1km Women\'s Cycle', '1800', 'Cycling', 'Velodrome']
    ]

    events = [{
        'name': row[0],
        'start': row[1],
        'sport': row[2],
        'venue': row[3]
    } for row in events_db]

    return events

'''
Get all the events for a certain sport - list it in order of start
'''
def all_events_sport(sportname):

    # TODO - update the events_db to get all events for a particular sport
    # Get all events for sport name.
    # Return the data (NOTE: look at the information, requires more than a simple select. NOTE ALSO: ordering of columns)
    # It is a list of lists
    # Chronologically order them by start

    # Format:
    # [
    #   [name, start, sport, venue_name]
    # ]

    conn=database_connect()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT event_name as name, event_start as start, sport.sport_name, event.sport_venue as venue_name
        FROM sport, event
        WHERE event.sport_id = sport.sport_id
        AND sport.sport_name = %s
        ORDER BY event.event_start
        """, (sportname,))

    event_db = cursor.fetchall()

    events_db = [
        ['1km Women\'s Cycle', '1800', 'Cycling', 'Velodrome'],
        ['1km Men\'s Cycle', '1920', 'Cycling', 'Velodrome']
    ]

    events = [{
        'name': row[0],
        'start': row[1],
        'sport': row[2],
        'venue': row[3],
    } for row in events_db]

    return events

'''
Get all of the events a certain member is participating in.
'''
def get_events_for_member(member_id):

    # TODO - update the events_db variable to pull from the database
    # Return the data (NOTE: look at the information, requires more than a simple select. NOTE ALSO: ordering of columns)
    #  [
    # It is a list of lists
    # Chronologically order them by start

    # Format:
    #   [name, start, sport, venue_name]
    # ]

    conn=database_connect()
    cursor = conn.cursor()
# i AM STRECHING FOR A SEC HAVE BEEN SITTING SINCE 8 OK

    cursor.execute("""
        SELECT event_name as name, event_start as start, sport.sport_name, event.sport_venue as venue_name
        FROM Sport, Participates, RunsEvent, Event
        WHERE Participates.event_id = Event.event_id AND
        Event.sport_id = Sport.sport_id AND
        RunsEvent.event_id = Event.event_id
        -- NOTE: would this work
        AND RunsEvent.member_id OR Participates.athlete_id = %s  //NOTE: THIS IS WRONG RUNSEVENT IS FOR OFFICIALS 1e!~h!t
        ORDER BY event.event_start
        """,(member_id,))

    event_db = cursor.fetchall()

    events_db = [
        ['1km Women\'s Cycle', '1800', 'Cycling', 'Velodrome'],
        ['1km Men\'s Cycle', '1920', 'Cycling', 'Velodrome']

    ]

    events = [{
        'name': row[0],
        'start': row[1],
        'sport': row[2],
        'venue': row[3],
    } for row in events_db]

    return events

'''
Get event information for a certain event
'''
def event_details(eventname):
    # TODO - update the event_db to get that particular event
    # Get all events for sport name.
    # Return the data (NOTE: look at the information, requires more than a simple select. NOTE ALSO: ordering of columns)
    # It is a list of lists
    # Chronologically order them by start

    # Format:
    #   [name, start, sport, venue_name]

    conn=database_connect()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT event_name as name, event_start as start, sport.sport_name, event.sport_venue as venue_name
        FROM Sport as sport, Event as event, RunsEvent
        WHERE event.sport_id = sport.sport_id
        AND event.event_name = %s
        ORDER BY event.event_start
        """, (eventname,))

    event_db = cursor.fetchall()



    event_db = ['1km Women\'s Cycle', '1800', 'Cycling', 'Velodrome']


    event = {
        'name' : event_db[0],
        'start': event_db[1],
        'sport': event_db[2],
        'venue': event_db[3]
    }

    return event



#####################################################
## Results
#####################################################

'''
Get the results for a given event.
'''
def get_results_for_event(event_name):

    # TODO - update the results_db to get information from the database!
    # Return the data (NOTE: look at the information, requires more than a simple select. NOTE ALSO: ordering of columns)
    # This should return a list of who participated and the results.

    # This is a list of lists.
    # Order by ranking of medal, then by type (e.g. points/time/etc.)

    # Format:
    # [
    #   [member_id, result, medal],
    #   ...
    # ]
    conn=database_connect()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT Participates.athlete_id as member_id, event.result_type as result, Participates.medal
        FROM Event as event JOIN Participates USING (event.event_id = Participates.event_id)
        WHERE event.event_name = %s
        ORDER BY Participates.medal
        """, (event_name,))

    results_db = cursor.fetchall()

    results_db = [
        ['1234567890', '10pts', 'Gold'], # We have started thinkin alike ahahhaha let me look
        ['8761287368', '8pts', 'Silver'],
        ['1638712633', '5pts', 'Bronze'],
        ['5873287436', '4pts', ''],
        ['6328743234', '4pts', '']]

    results =[{
        'member_id': row[0],
        'result': row[1],
        'medal': row[2]
    } for row in results_db]

    return results

'''
Get all the officials that participated, and their positions.
'''
def get_all_officials(event_name):
    # TODO
    # Return the data (NOTE: look at the information, requires more than a simple select. NOTE ALSO: ordering of columns)
    # This should return a list of who participated and their role.

    # This is a list of lists.

    # [
    #   [member_id, role],
    #   ...
    # ]
    conn=database_connect()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT RunsEvent.member_id, RunsEvent.role
        FROM Event, RunsEvent
        WHERE RunsEvent.event_id = Event.event_id
        AND event.event_name = %s
        """, (event_name,))

    officials_db = cursor.fetchall()
    officials_db = [
        ['1234567890', 'Judge'],
        ['8761287368', 'Medal Holder'],
        ['1638712633', 'Random Bystander'],
        ['5873287436', 'Umbrella Holder'],
        ['6328743234', 'Marshall']
    ]

    officials = [{
        'member_id': row[0],
        'role': row[1]
    } for row in officials_db]


    return officials

# =================================================================
# =================================================================

#  FOR MARKING PURPOSES ONLY
#  DO NOT CHANGE

def to_json(fn_name, ret_val):
    return {'function': fn_name, 'res': json.dumps(ret_val)}

# =================================================================
# =================================================================


# =================================================================
# Utility Functions
# =================================================================

def create_full_timestamp(date, hour):
    full_timestamp=date+' '+hour
    return full_timestamp

def are_passwords_hashed():
    try:
        conn=database_connect()
        cursor = conn.cursor()
        # Find the users data
        cursor.execute("""SELECT column_name
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'member' AND
            column_name = 'pass_word' AND
            character_maximum_length = 255""")

        passwords_hashed = cursor.rowcount == 1
        return passwords_hashed==1
    finally:
        cursor.close()
        conn.close()

def create_individual_hash(args):
    bc,rounds,pwd,mem_id=args
    hashed=bc.generate_password_hash(pwd,rounds).decode('utf-8')
    return [hashed, mem_id]

def hash_passwords():
    # CONFIG
    num_hashing_rounds=4 # (higher means slower but more secure)
    print_frequency=200
    try:
        conn=database_connect()
        cursor = conn.cursor()
        print('Password hashing will now commence. This process only needs to run once.')

        #get every password
        cursor.execute("""SELECT pass_word, member_id
            FROM member
            WHERE pass_word IS NOT NULL""")
        valid_user_passwords=cursor.fetchall()

        #change columns
        cursor.execute("""ALTER TABLE member DROP pass_word""")
        cursor.execute("""ALTER TABLE member ADD pass_word VARCHAR(255) DEFAULT NULL""")

        #multithreaded hash creation
        print('creating password hashes expect this to take up to a minute (only needs to run once)')
        bc = Bcrypt(None)
        from multiprocessing.dummy import Pool as ThreadPool
        pool = ThreadPool(4)
        password_arguments = [[bc,num_hashing_rounds,member[0],member[1]] for member in valid_user_passwords]
        password_changes = pool.map(create_individual_hash,password_arguments)

        cursor.execute("""
            CREATE OR REPLACE FUNCTION insert_hashed(hashed_pwd VARCHAR(255), mem_id CHAR(10))
            RETURNS void AS $$
            BEGIN
                UPDATE member
                SET pass_word = hashed_pwd
                WHERE member_id=mem_id;
            END;
            $$ LANGUAGE plpgsql;
            """)

        print('prepared statement created')
        print('inserting hashed passwords to database (expect this to also take a while)')
        sql_statement="""SELECT insert_hashed(%s,%s)""" #insert changed passwords

        cursor.executemany(sql_statement,password_changes)
        print('Hashed passwords have been inserted into the database.')
        print('Removing gross ugly unhashed plaintext passwords from the database.')


        conn.commit()

        print('changes have been commited')
        print('The alter table statements are executed from within the python code (to ensure that no unhashed passwords remain after hashing is complete).')
        print('To verify that the hashed passwords are stored in the database you can just look in the member table.')
    finally:
        cursor.close()
        conn.close()

# =================================================================
# =================================================================
