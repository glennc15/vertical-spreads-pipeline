'''



dags for getting the most recent timestamp from the postgres database.
then quering mongo for the next record.


# create_expiration_table >> poll_pg_expiration >> get_spot_record_mongo >> build_expiration_records_mongo
create_expiration_table >> poll_pg_expiration >> get_spot_record_mongo >> build_expiration_records_mongo


create_expiration_table: creates the expiration table if it doesn't exist. Returns nothing
poll_pg_expiration: returns the latest timestamp from the postgres expirations table. Returns None if there are no expiration records yet.
get_spot_record_mongo: returns the next spot record from mongo. Or returns the first spot record is poll_pg_ts is None.



'''