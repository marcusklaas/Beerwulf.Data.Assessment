import csv

def map_helper(file_name, row_mapper):
    map = {}
    with open(file_name, 'r') as csvfile:
        for row in csv.reader(csvfile, delimiter='|'):
            map[int(row[0])] = row_mapper(row)
    return map

# helper functions for customer balance classification
def balance_status(balance):
    real_balance = float(balance)
    if real_balance == 0.0:
        return 'settled'
    if real_balance > 0.0:
        return 'credit'
    return 'debit'

def import_helper(cursor, file_name, table_name, row_mapper):
    table_width = 0
    rows = []

    with open(file_name, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter='|')

        for row in reader:
            new_row = row_mapper(row)
            table_width = len(new_row)
            rows.append(new_row)

    if table_width > 0:
        query = f"INSERT INTO {table_name} VALUES ({', '.join(['?'] * table_width)})"
        cursor.executemany(query, rows)
