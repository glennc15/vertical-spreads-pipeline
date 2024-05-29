'''

tasks:

create_vertical_tables: runs an sql file to build all required tables.

Each following task needs to know the timestamp for the current spot record:
all intermediate sql files are stored in temp/vertical_sql_files

build_spot_record:
    1) get the latest record spot record from postgres.
    2) get the next spot record from mongo
    3) write the new spot record to an sql file

    if no spot records exists in postgres:
        a) get the first spot record from mongo
        b) write the new spot record an sql file

write_spot_record:
    Postgres Operator
    1) writes spot record using the sql file generated in build_spot_record

build_expiration_records:
    1) get the latest spot record from postgres.
    2) using the postgres spot record timestamp, match the mongo-lake spot record
    3) get all options from mongo-lake using the spot record _id
    4) if no option records match with _id, try matching with the timestamp
    5) if still no option records then no expiration records.
    6) get all expiration from the options
    7) write expiration records to an sql file

write_expiration_records:
    Postgres Operator
    1) writes expiration records using the sql file generated in build_expiration_records

These next tasks will branch out from write_expiraiton_records and be ran in parallel:

build_bull_call_spreads:
    1) get the latest spot record from postgres.
    2) get the latest expiration records from postgres.
    3) get the matching spot record from mongo-lake
    4) for each mongo-lake _id and expiraiotn:
        a) get options using the mongo-lake _id and expiration
        b) build bull call spreads
        c) write spreads to an sql file

write_bull_call_spreads:
    Postgres Operator:
    1) write the bull call spreads using the sql file generated by build_bull_call_spreads.


build_bear_call_spreads/write_bear_call_spreads: same as build_bull_call_spreads/write_bull_call_spreads
build_bull_pull_spreads/write_bull_put_spreads: same as build_bull_call_spreads/write_bull_call_spreads
build_bear_pull_spreads/write_bear_put_spreads: same as build_bull_call_spreads/write_bull_call_spreads


remove_all_temp_files:
    removes all sql files


------------------------------------------------------------------------------------

dags for getting the most recent timestamp from the postgres database.
then quering mongo for the next record.


# create_expiration_table >> poll_pg_expiration >> get_spot_record_mongo >> build_expiration_records_mongo
create_expiration_table >> poll_pg_expiration >> get_spot_record_mongo >> build_expiration_records_mongo


create_expiration_table: creates the expiration table if it doesn't exist. Returns nothing
poll_pg_expiration: returns the latest timestamp from the postgres expirations table. Returns None if there are no expiration records yet.
get_spot_record_mongo: returns the next spot record from mongo. Or returns the first spot record is poll_pg_ts is None.



'''