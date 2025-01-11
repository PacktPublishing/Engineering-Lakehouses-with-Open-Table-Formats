INSTALL 'icebereg';

LOAD 'iceberg';

-- Read an Iceberg Table:
select * from iceberg_scan('/Users/dipankarmazumdar/Downloads/customers', allow_moved_paths = true); 

-- Use Conditional Logic in DuckDB to create custom lables based on charges:
SELECT first_name, last_name, charges, 

         CASE WHEN charges > 150 THEN 'High spender' 

              WHEN charges BETWEEN 100 AND 150 THEN 'Medium spender' 

              ELSE 'Low spender' END AS spending_category 

FROM iceberg_scan('/Users/dipankarmazumdar/Downloads/customers', allow_moved_paths = true); 

-- Read Iceberg table metadata:
select * from iceberg_metadata('/Users/dipankarmazumdar/Downloads/customers',allow_moved_paths = true); 

-- Fetch all snapshots for the Iceberg table:
SELECT * 
	FROM iceberg_snapshots('/Users/dipankarmazumdar/Downloads/customers'); 