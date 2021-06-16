import sqlite3
import csv

def main():
    db = connect_to_db()
    cur = db.cursor()

    regions = load_regions()
    nations = load_nations(regions)

    # load dimension tables
    load_customers(cur, nations)
    load_suppliers(cur, nations)
    load_parts(cur)
    load_partsupps(cur)
    load_orders(cur)

    # load the fact table
    load_lineitems(cur)

    db.commit()
    db.close()

def connect_to_db():
    return sqlite3.connect('test.db')

# returns a region_id -> regionname map
def load_regions():
    map = {}
    with open('region.tbl', 'r') as csvfile:
        regionreader = csv.reader(csvfile, delimiter='|')
        for row in regionreader:
            map[int(row[0])] = row[1]
    return map

# returns a nation_id -> (nationname, regionname) map
def load_nations(regions):
    map = {}
    with open('nation.tbl', 'r') as csvfile:
        nationreader = csv.reader(csvfile, delimiter='|')
        for row in nationreader:
            map[int(row[0])] = (row[1], regions[int(row[2])])
    return map

def load_customers(cursor, nation_map, region_map):
    # create the table
    query = '''CREATE TABLE customers
    (c_custkey integer PRIMARY KEY, c_name text, c_address text, c_phone text, c_acctbal real,
     c_balancestatus text, c_mktsegment text, c_comment text, c_nationname text,
     c_regionname text)
    '''
    cursor.execute(query)

def load_suppliers(cursor, nation_map, region_map):
    query = '''CREATE TABLE suppliers
    (s_suppkey integer PRIMARY KEY, s_name text, s_address text, s_phone text, s_acctbal real,
     s_comment text, s_nationname text, s_regionname text)
    '''
    cursor.execute(query)

def load_parts(cursor):
    query = '''CREATE TABLE parts
    (p_partkey integer PRIMARY KEY, p_name text, p_mfgr text, p_brand text, p_type text,
     p_size integer, p_container text, p_retailprice real, p_comment text)
    '''
    cursor.execute(query)

def load_partsupps(cursor):
    query = '''CREATE TABLE partsupps
    (ps_id integer PRIMARY KEY, ps_availqty integer, ps_supplycost real, ps_comment text)
    '''
    cursor.execute(query)

def load_orders(cursor):
    query = '''CREATE TABLE orders
    (o_orderkey integer PRIMARY KEY, o_orderstatus text, o_totalprice real, o_orderdate text,
     o_orderpriority text, o_cleark text, o_shippriority text, o_comment text)
    '''
    cursor.execute(query)

# main fact table
def load_lineitems(cursor):
    query = '''CREATE TABLE lineitems
    (l_id integer PRIMARY KEY, l_orderkey integer, l_ps_id integer, l_custkey integer,
     l_partkey integer, l_suppkey integer, l_linenumber integer, l_quantity integer,
     l_extendedprice real, l_discount real, l_revenue real, l_tax real, l_returnflag text,
     l_linestatus text, l_shipdate text, l_commitdate text, l_receiptdate text,
     l_shipinstruct text, l_shipmode text, l_comment text,
     FOREIGN KEY(l_orderkey) REFERENCES orders(o_orderkey),
     FOREIGN KEY(l_ps_id) REFERENCES partsupps(ps_id),
     FOREIGN KEY(l_custkey) REFERENCES customers(c_custkey),
     FOREIGN KEY(l_partkey) REFERENCES parts(p_partkey),
     FOREIGN KEY(l_suppkey) REFERENCES suppliers(s_suppkey))
    '''
    cursor.execute(query)

# we may wish to import some functions defined here without running
# the ETL, so only execute when this file is the main routine
if __name__ == "__main__":
    main()
