import psycopg2                     # Library to connect Python to PostgreSQL
import csv                          # Library to read CSV files
from config import load_config      # Your config.py to load DB credentials

allowed_search_attributes = ['first_name', 'last_name', 'number']

def load_sql_files():
    config = load_config()
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cursor:
                for file in ['functions.sql', 'procedures.sql']:
                    with open(file, 'r') as f:
                        cursor.execute(f.read())
                        # print(f'[Success] File {file} have been imported!')
            conn.commit()
    except (psycopg2.DatabaseError, Exception) as error:
        if conn:
            conn.rollback()
        # print(error)
                

# insertion
# === Step 1: Function to insert one contact row into DB ===
def insert_contact(contact: dict):
    config = load_config()
    new_id = None
    """
    Inserts a single contact into the contacts table.
    cursor  : psycopg2 cursor object
    contact : dictionary with keys: first_name, last_name, phone_number, email, additional_info
    """
    # SQL statement with placeholders (%s) for parameters
    try:
        sql = """
            INSERT INTO contacts(
                contact_first_name,
                contact_last_name,
                contact_number,
                contact_email,
                contact_extra_info
            )
            VALUES (%s, %s, %s, %s, %s)
            RETURNING contact_id;
        """

        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cursor:
                # Execute SQL with data from 'contact' dictionary
                cursor.execute(sql, (
                    contact['first_name'],
                    contact.get('last_name', None),   # use None if missing
                    contact['phone_number'],
                    contact.get('email', None),
                    contact.get('additional_info', None)
                ))
                new_id = cursor.fetchone()[0]
                print(f"[Success] Successfully inserted a contact with id: {new_id}")
            conn.commit()
    except Exception as error:
        if conn:
            conn.rollback()
        print(error)
    finally:
        # Fetch the generated contact_id (Postgres serial primary key)
        return new_id

# === Step 2: Main function to read CSV and insert data ===
def import_contacts_from_csv(csv_file_path):
    """
    Reads a CSV file and inserts all contacts into the database.
    """
    # Load database connection parameters from config.py
    config = load_config()
    
    try:
        # Open connection using 'with' so it closes automatically
        with psycopg2.connect(**config) as conn:
            # Open a cursor to execute SQL commands
            with conn.cursor() as cur:
                # Open CSV file
                with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)  # Reads rows as dictionaries
                    inserted_count = 0
                    
                    # Loop through each row in CSV
                    for row in reader:
                        # Insert each contact into DB
                        new_id = insert_contact(row)
                        inserted_count += 1
                        print(f"[I] Inserted contact {row['first_name']} with ID {new_id}")
                
                # Commit all changes to the database
                conn.commit()
                
        print(f"[Success] ✅ Successfully inserted {inserted_count} contacts from CSV.")
        
    except (Exception, psycopg2.DatabaseError) as error:
        if conn:
            conn.rollback()
        print("[Error]:", error)


# update
def update_contact(contact_change_attribute, contact_new_info, contact_number, contact_first_name):
    config = load_config()
    updated_row_count = 0

    if contact_number and contact_first_name == None:
        sql = f"""
            update contacts 
            set {contact_change_attribute}='{contact_new_info}'
            where contact_number='{contact_number}'
        """
    else:
        sql = f"""
            update contacts 
            set {contact_change_attribute}='{contact_new_info}'
            where contact_first_name='{contact_first_name}'
        """
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, None)
                updated_row_count = cursor.rowcount
            conn.commit()
            if updated_row_count == 0:
                print("[Error] There is no row updated. Make sure you have inputted the data correctly!")
            else:
                print(f"[Success] The update operation have completed successfully: attribute {contact_change_attribute}'s value -> {contact_new_info}")
    except (Exception, psycopg2.DatabaseError) as error:
        if conn:
            conn.rollback() # transaction method
        print(error)
    finally:
        return updated_row_count



# query
allowed_attributes = ['contact_id', 'contact_first_name', 'contact_last_name', 'contact_number', 'contact_email', 'contact_extra_info']
allowed_sort_types = ['asc', '+', '-', 'desc', '']
def get_info(filter, sort_key, sort_type, sort_aggregate_value):
    config = load_config()
    if filter.lower() == '*' or filter.lower() == 'all':
        if sort_key == '':
            sql = """
                select * from contacts 
            """
        elif (sort_key not in allowed_attributes) or (sort_type not in allowed_sort_types):
            print("Key/type error: Please ensure you've typed sort_key and sort_type correctly.")
            print(f"Arguments you've written: sort_key: {sort_key}, sort_type: {sort_type}",)
            print("Allowed attributes:", *allowed_attributes)
            print("Allowed sort_types:", *allowed_sort_types)
        else:
            sql = f"""
                select * from contacts order by {sort_key} {sort_type}
            """
    elif filter.lower() in allowed_attributes:
        sql = f"""
            select * from contacts where {filter}='{sort_aggregate_value}'
        """
    else:
        print("[Error]: The filter isn't correct. You can choose: '*' (diplaying all data), 'key_name' and needed key value")
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                print("The number of parts: ", cursor.rowcount)
                row = cursor.fetchone()
                while row is not None:
                    print(row)
                    row = cursor.fetchone()
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        if conn:
            conn.rollback()
        print(error)


#delete
def delete_contact(first_name, number):
    config = load_config()
    if first_name and number == None:
        sql = """
            delete from contacts where contact_first_name=%s
        """
    else:
        sql = """
            delete from contacts where contact_number=%s
        """
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cursor:
                if first_name and number == None:
                    cursor.execute(sql, (first_name,))
                    deleted_row_count = cursor.rowcount
                    if deleted_row_count == 0:
                        print("[Error] No contact deleted. Please ensure you've typed the data correct!")
                    else:
                        print(f"[Success] Successfully deleted row with first name '{first_name}'")
                else:
                    cursor.execute(sql, (number,))
                    deleted_row_count = cursor.rowcount
                    if deleted_row_count == 0:
                        print("[Error] No contact deleted. Please ensure you've typed the data correct!")
                    else:
                        print(f"[Success] Successfully deleted row with number '{number}'")
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        if conn:
            conn.rollback()
        print(error)

# created 29.03.2026
def insert_contact2(contact: dict):
    config = load_config()
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cursor:
                first_name = contact['first_name']
                last_name = contact.get('last_name', None)   # use None if missing
                phone_number = contact['phone_number']
                email = contact.get('email', None)
                extra_info = contact.get('additional_info', None)
                sql = "call insert_user(%s, %s, %s, %s, %s)"
                cursor.execute(sql, (first_name, last_name, phone_number, email, extra_info))
            conn.commit()
            print(f"[Success]: The contact {first_name} {last_name} was inserted/updated!")
    except (psycopg2.DatabaseError, Exception) as error:
        if conn:
            conn.rollback()
        print(error)

def insert_multiple_contacts(users: list):
    """
    Inserts multiple contacts via the PostgreSQL 'multiple_insertion' procedure.
    
    users: list of dictionaries, each dict must have keys:
           'first_name', 'last_name', 'phone_number', 'email', 'extra_info'
    """
    config = load_config()

    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cursor:

                # Build a list of composite type strings
                # PostgreSQL array literal format: ARRAY[ROW(...), ROW(...)]
                array_elements = []
                for u in users:
                    first_name = u['first_name'] or ''
                    last_name = u.get('last_name', '')
                    phone = u['phone_number']
                    email = u.get('email', '')
                    extra = u.get('extra_info', '')
                    # Create a ROW() literal for each user
                    array_elements.append(f"ROW('{first_name}','{last_name}','{phone}','{email}','{extra}')::user_type")

                # Join into PostgreSQL array syntax
                array_literal = "ARRAY[" + ",".join(array_elements) + "]"

                # Call the procedure
                sql = f"CALL multiple_insertion({array_literal});"
                cursor.execute(sql)

            conn.commit()
            print(f"[Success] {len(users)} contacts processed via multiple_insertion procedure.")

    except (psycopg2.DatabaseError, Exception) as error:
        if conn:
            conn.rollback()
        print("[Error]:", error)

        

def delete_contact2(first_name, last_name, phone_number):
    config = load_config()
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cursor:
                sql = "call delete_user(%s, %s, %s)"
                cursor.execute(sql, (first_name, last_name, phone_number))
                print(f"[Warning]: Command executed successfully. Recheck: with invalid values the contact may not be deleted.")
            conn.commit()
    except (psycopg2.DatabaseError, Exception) as error:
        if conn:
            conn.rollback()
        print(error)

def search_by_pattern(pattern_type, pattern_value):
    config = load_config()
    sql = f"""
        select get_by_pattern(%s, %s);
    """
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (pattern_type, pattern_value))
                rows = cursor.fetchall()
                print(f"Number of rows: {len(rows)}")
                if len(rows) == 0:
                    print("Please ensure you've typed the correct attribute_type (first_name, last_name, or number).")
                for row in rows:
                    print(row)
    except (psycopg2.DatabaseError, Exception) as error:
        if conn:
            conn.rollback()
        print(error)

def query_pagination(rows_per_page, page_index):
    config = load_config()
    sql = f"""
        select query_pagination(%s, %s);
    """
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (rows_per_page, page_index))
                rows = cursor.fetchall()
                print(f"Number of rows: {len(rows)}")
                for row in rows:
                    print(row)
    except (psycopg2.DatabaseError, Exception) as error:
        if conn:
            conn.rollback()
        print(error)











# main body
print("Welcome to the terminal of parsing data. Here you can insert, update, get information, or delete your contacts")
allowed_attributes = ['contact_id','contact_first_name', 'contact_last_name', 'contact_number', 'contact_email', 'contact_extra_info']
while True:
    load_sql_files()
    contact = {}
    command = input("Insert a command (insert/update/get/search/page/delete/exit): ").lower()
    if command == 'insert' or command == 'i':
        insertion_type = input("Enter the type of insertion (csv - for csv, b - basic): ").lower()
        if insertion_type == '' or insertion_type == 'basic' or insertion_type == 'b':
            first_name = input("Input the first_name: ")
            if first_name == '':
                print("[Error] First name can't be empty! Restart the session.")
                break

            last_name = input("Input the last_name (optional): ")
        
            phone_number = input("Input the phone_number: ")
            if phone_number == '':
                print("[Error] Phone number can't be empty! Restart the session.")
                break

            email = input("Input the email (optional): ")
            additional_info = input("Input the additional information (optional): ")

            contact["first_name"] = first_name
            contact["last_name"] = last_name if last_name != '' else None
            contact["phone_number"] = phone_number
            contact["email"] = email if email != '' else None
            contact["additional_info"] = additional_info if additional_info != '' else None
            insert_contact2(contact)

        elif insertion_type == 'csv' or insertion_type == 'c':
            path = input("index a path where the .csv file is located \n(only the name of the file if it's located in the same direction as this file): ")
            import_contacts_from_csv(path)

    elif command == 'update' or command == 'u':
        changing_attribute = input(f"Input which attribute (column) you want to change\nThe list of all attributes: {allowed_attributes[1:]}\n")
        if changing_attribute not in allowed_attributes:
            print(f"[Error] The attribute {changing_attribute} doesn't found. Please ensure you've typed the attribute correctly\nThe list of all attributes: {allowed_attributes}\n")
        else:
            new_value = input("Input a value you want to set: ")
            anchor_attribute = input("Choose by which attribute do you want to search (number/first name): ")
            if anchor_attribute.lower() == "number" or anchor_attribute.lower() == "num":
                phone_number = input("Now input a phone number: ")
                update_contact(changing_attribute, new_value, phone_number, contact_first_name=None)
            elif anchor_attribute.lower() == "first name" or anchor_attribute.lower() == "first_name" or anchor_attribute.lower() == "name":
                first_name = input("Now input a first name: ")
                phone_number = None
                update_contact(changing_attribute, new_value, phone_number, first_name)
            else:
                print("[Error] You've chosen incorrect search_attribute_type! Please ensure you inputted correct attribute: 'first name' or 'number")
    
    elif command == "get" or command == "g":
        filter = input(f"Input the filter you want to apply\n'*' - displays all data\nOr, you can write attribute's name\nList of all attributes: {allowed_attributes}\n").lower()
        if filter == '':
            print("[Error] You didn't input the filter! Please, retype it again.")
        sort_key = input("Now, input a key by which your data will be sorted (you can ignore this line if you've already parsed key in filter's line): ")
        sort_type = input("Input a type of sort (asc/desc): ")
        sort_aggregation_value = input("Input a value that will specify and filter the data (optional): ")
        get_info(filter, sort_key, sort_type, sort_aggregation_value)

    elif command == "search":
        attribute = input("Now input the attribute by which you want to search the contact (first_name, last_name, or number): ")
        pattern_value = input("Now input the search value: ")
        search_by_pattern(attribute, pattern_value)

    elif command == "page":
        rows_per_page = int(input("Input a number of rows for one page: "))
        page_index = int(input("Input the page number (index): "))
        query_pagination(rows_per_page, page_index)

    elif command == "delete" or command == "d":
        first_name = input("Input a first name, which contact will be deleted: ")
        last_name = input('Input a last name, which contact will be deleted: ')
        number = input("Now input a number (for case if the name will be invalid): ")
        delete_contact2(first_name, last_name, number)

    elif command == "quit" or command == "q" or command == "exit":
        print("[Exit] Exiting the programm...")
        break
    else:
        print("[Error] The command is wrong! Please assure you've written the command correct\n")