import sqlite3
import csv
from helpers import balance_status, map_helper, import_helper

def main():
    db = connect_to_db()
    cur = db.cursor()

    regions = load_regions()
    nations = load_nations(regions)

    # load dimension tables
    load_customers(cur, nations)
    load_suppliers(cur, nations)
    load_parts(cur)
    ps_part_supp_map = load_partsupps(cur)
    order_customer_map = load_orders(cur)

    # load the fact table
    load_lineitems(cur, order_customer_map, ps_part_supp_map)

    db.commit()
    db.close()

def connect_to_db():
    return sqlite3.connect('test.db')

# returns a region_id -> regionname map
def load_regions():
    return map_helper('region.tbl', lambda row: row[1])

# returns a nation_id -> (nationname, regionname) map
def load_nations(regions):
    return map_helper('nation.tbl', lambda row: (row[1], regions[int(row[2])]))

def load_customers(cursor, nation_map):
    # create the table
    query = '''CREATE TABLE customers
    (c_custkey integer PRIMARY KEY, c_name text, c_address text, c_phone text, c_acctbal real,
     c_balancestatus text, c_mktsegment text, c_comment text, c_nationname text,
     c_regionname text)
    '''
    cursor.execute(query)

    def mapper(row):
        nation_tuple = nation_map[int(row[3])]
        return (
            row[0],
            row[1],
            row[2],
            row[4],
            row[5],
            balance_status(row[5]),
            row[6],
            row[7],
            nation_tuple[0],
            nation_tuple[1],
        )

    import_helper(cursor, 'customer.tbl', 'customers', mapper)

def load_suppliers(cursor, nation_map):
    query = '''CREATE TABLE suppliers
    (s_suppkey integer PRIMARY KEY, s_name text, s_address text, s_phone text, s_acctbal real,
     s_comment text, s_nationname text, s_regionname text)
    '''
    cursor.execute(query)
    
    def mapper(row):
        nation_tuple = nation_map[int(row[3])]
        return (
            row[0],
            row[1],
            row[2],
            row[4],
            row[5],
            row[6],
            nation_tuple[0],
            nation_tuple[1],
        )

    import_helper(cursor, 'supplier.tbl', 'suppliers', mapper)

def load_parts(cursor):
    query = '''CREATE TABLE parts
    (p_partkey integer PRIMARY KEY, p_name text, p_mfgr text, p_brand text, p_type text,
     p_size integer, p_container text, p_retailprice real, p_comment text)
    '''
    cursor.execute(query)

    import_helper(cursor, 'part.tbl', 'parts', lambda row: row[0:-1])

def load_partsupps(cursor):
    query = '''CREATE TABLE partsupps
    (ps_id integer PRIMARY KEY, ps_availqty integer, ps_supplycost real, ps_comment text)
    '''
    cursor.execute(query)

    part_supp_map = {}
    
    def mapper(row):
        part_supp_map[int(row[0])] = (int(row[1]), int(row[2]))
        return (
            row[0],
            row[3],
            row[4],
            row[5]
        )

    import_helper(cursor, 'partsupp.tbl', 'partsupps', mapper)

    return part_supp_map

def load_orders(cursor):
    query = '''CREATE TABLE orders
    (o_orderkey integer PRIMARY KEY, o_orderstatus text, o_totalprice real, o_orderdate text,
     o_orderpriority text, o_cleark text, o_shippriority text, o_comment text)
    '''
    cursor.execute(query)

    customer_map = {}
    
    def mapper(row):
        customer_map[int(row[0])] = int(row[1])
        return [row[0]] + row[2:-1]
    
    import_helper(cursor, 'orders.tbl', 'orders', mapper)

    return customer_map

# main fact table
def load_lineitems(cursor, order_customer_map, ps_part_supp_map):
    query = '''CREATE TABLE lineitems
    (l_id integer PRIMARY KEY, l_orderkey integer, l_ps_id integer, l_custkey integer,
     l_partkey integer, l_suppkey integer, l_revenue real, l_linenumber integer, l_quantity integer,
     l_extendedprice real, l_discount real, l_tax real, l_returnflag text,
     l_linestatus text, l_shipdate text, l_commitdate text, l_receiptdate text,
     l_shipinstruct text, l_shipmode text, l_comment text,
     FOREIGN KEY(l_orderkey) REFERENCES orders(o_orderkey),
     FOREIGN KEY(l_ps_id) REFERENCES partsupps(ps_id),
     FOREIGN KEY(l_custkey) REFERENCES customers(c_custkey),
     FOREIGN KEY(l_partkey) REFERENCES parts(p_partkey),
     FOREIGN KEY(l_suppkey) REFERENCES suppliers(s_suppkey))
    '''
    cursor.execute(query)
    
    def mapper(row):
        part_supp_tuple = ps_part_supp_map[int(row[2])]
        return [
            row[0],
            row[1],
            row[2],
            order_customer_map.get(int(row[1]), 'NULL'),
            part_supp_tuple[0],
            part_supp_tuple[1],
            float(row[5]) * (1 - float(row[6])), # REVENUE
        ] + row[3:-1]

    import_helper(cursor, 'lineitem.tbl', 'lineitems', mapper)

# we may wish to import some functions defined here without running
# the ETL, so only execute when this file is the main routine
if __name__ == "__main__":
    main()
