import duckdb
import numpy as np
con = duckdb.connect('/duckdb_data/warehouse.duckdb')

print('=== Total Orders ===')
print(con.execute('SELECT COUNT(*) FROM orders').fetchall())

print('\n=== Sample Orders ===')
print(con.execute('SELECT * FROM orders LIMIT 5').fetchdf())

print('\n=== Revenue by Product ===')
print(con.execute('''
    SELECT
        product,
        COUNT(*)        as total_orders,
        SUM(price)      as total_revenue,
        AVG(price)      as avg_price
    FROM orders
    GROUP BY product
    ORDER BY total_revenue DESC
''').fetchdf())

con.close()