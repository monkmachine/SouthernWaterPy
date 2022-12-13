import argparse
import os
import sqlite3

# Script to find anomalies in beachbuoy data harvested by SoutherWater.py
# It needs multiple csv files from SoutherWater.py created at different times
# so that changes in event records can be tracked.

# Example
# -------
# $ python3 compare.py *.csv
# Creating db file :memory:
# ✅ Read 16936 records from 20221117-185553_Spills.csv
# ✅ Read 16934 records from 20221117-185553_Spills.edited.csv
# ✅ Read 17802 records from 20221124-102959_Spills.csv

# Events with duration decreases by delta
# ---------------------------------------
# eventId, eventStop, bathingSite, outfallName, current duration, duration decrease
# 📉 667549, 2022-11-17T01:07:00, CHICHESTER HARBOUR, THORNHAM, 206, -3163
# 📉 667463, 2022-11-15T11:28:00, BEMBRIDGE, HILLWAY BEMBRIDGE, 143, -2912
# 📉 667462, 2022-11-17T03:08:00, LANGSTONE HARBOUR, BUDDS FARM HAVANT NO2, 563, -2852
# 📉 668438, 2022-11-16T05:11:00, BOGNOR REGIS (ALDWICK), WEST PARK BOGNOR REGIS, 111, -2653
# 📉 669160, 2022-11-16T13:56:00, HERNE BAY CENTRAL, SWALECLIFFE NO2, 199, -1736
# 📉 667222, 2022-11-18T21:13:00, RYDE, APPLEY PARK RYDE TRANSFER, 2345, -1258
# 📉 667174, 2022-11-15T19:11:00, COWES, TERMINUS ROAD COWES 2, 867, -1141
# 📉 668767, 2022-11-17T10:30:00, CHICHESTER HARBOUR, KINGS ROAD EMSWORTH, 1091, -1065
# 📉 670197, 2022-11-16T21:31:00, HILLHEAD, NEWGATE LANE PEEL COMMON, 128, -517
# 📉 669733, 2022-11-16T23:15:00, SHANKLIN, HOPE BEACH SHANKLIN NEW, 419, -496
# 📉 670552, 2022-11-17T02:55:00, PEVENSEY BAY, MONTAGUE WAY WESTHAM, 480, -480
# 📉 669729, 2022-11-16T20:08:00, MILFORD-ON-SEA, BECTON LANE BARTON ON SEA, 71, -443
# 📉 667350, 2022-11-17T11:20:00, RYDE, PRINCE CONSORT RYDE NEW, 3085, -395
# 📉 670721, 2022-11-16T22:01:00, SHEERNESS, BRIELLE WAY WESTMINSTER, 16, -359
# 📉 669745, 2022-11-17T01:48:00, COWES, ALBANY ROAD EAST COWES, 558, -301
# 📉 667449, 2022-11-15T12:02:00, COWES, ALBANY ROAD EAST COWES, 171, -250
# 📉 669936, 2022-11-17T16:49:00, BEXHILL, GALLEY HILL BEXHILL, 1249, -221
# 📉 671347, 2022-11-17T05:40:00, HERNE BAY CENTRAL, EDDINGTON LANE HERNE BAY NO2, 220, -185
# 📉 669718, 2022-11-17T13:12:00, COWES, TERMINUS ROAD COWES 2, 1279, -176
# 📉 671175, 2022-11-17T01:01:00, SHEERNESS, SOUTH STREET QUEENBOROUGH, 24, -171
# 📉 670261, 2022-11-17T11:18:00, BEXHILL, BEXHILL DOWN, 942, -159
# 📉 671756, 2022-11-17T11:14:00, LITTLESTONE, QUEENS ROAD NEW ROMNEY, 479, -151
# 📉 670269, 2022-11-16T19:58:00, HASTINGS PELHAM BEACH, SEASIDE ROAD HASTINGS, 26, -139
# 📉 671987, 2022-11-17T10:32:00, HERNE BAY CENTRAL, GAINSBOROUGH DRIVE HERNE BAY, 204, -114
# 📉 670917, 2022-11-17T03:39:00, LANCING BEACH GREEN, EAST WORTHING NO2, 170, -109
# 📉 669205, 2022-11-17T17:08:00, FELPHAM, ALDWICK AVENUE BOGNOR, 1856, -94
# 📉 667286, 2022-11-15T12:56:00, SOUTHSEA EAST, PIER ROAD SOUTHSEA, 339, -81
# 📉 668677, 2022-11-17T10:59:00, LITTLEHAMPTON, FORD ROAD ARUNDEL, 1944, -76
# 📉 670484, 2022-11-16T22:53:00, SALTDEAN, PORTOBELLO BRIGHTON NO1, 195, -73
# 📉 669162, 2022-11-16T11:07:00, HERNE BAY CENTRAL, EDDINGTON LANE HERNE BAY NO2, 82, -68
# 📉 668834, 2022-11-17T12:42:00, MIDDLETON-ON-SEA, BOGNOR MAIN, 1988, -64
# 📉 671215, 2022-11-17T03:00:00, HERNE BAY CENTRAL, GAINSBOROUGH DRIVE HERNE BAY, 132, -60
# 📉 669740, 2022-11-16T23:59:00, SOUTHSEA EAST, PIER ROAD SOUTHSEA, 456, -60
# 📉 672490, 2022-11-16T19:48:00, LANGSTONE HARBOUR, BUDDS FARM HAVANT NO1, 2, -43
# 📉 670074, 2022-11-16T20:03:00, ST LEONARDS, ST HELENS DOWN HASTINGS, 79, -39
# 📉 672202, 2022-11-17T11:17:00, HERNE BAY CENTRAL, EDDINGTON LANE HERNE BAY NO2, 131, -34
# 📉 671867, 2022-11-17T09:16:00, LEYSDOWN, BARROWS BROOK EASTCHURCH, 253, -31
# 📉 670876, 2022-11-16T23:32:00, ST LEONARDS, ST HELENS DOWN HASTINGS, 48, -31
# 📉 670064, 2022-11-16T20:00:00, LEE-ON-SOLENT, QUEENS ROAD LEE ON THE SOLENT, 87, -31
# 📉 670545, 2022-11-17T00:39:00, SEAFORD, BEACH ROAD NEWHAVEN, 224, -29
# 📉 670567, 2022-11-16T21:15:00, RYDE, AUGUSTA ROAD RYDE, 22, -29
# 📉 670825, 2022-11-16T23:01:00, LEYSDOWN, BARROWS BROOK EASTCHURCH, 52, -28
# 📉 669815, 2022-11-16T23:48:00, SEAGROVE, GREAT PRESTON ROAD RYDE, 427, -27
# 📉 669948, 2022-11-16T17:57:00, ST LEONARDS, ST HELENS DOWN HASTINGS, 15, -27
# 📉 670112, 2022-11-16T19:42:00, COWES, EGYPT POINT COWES, 59, -24
# 📉 669997, 2022-11-16T18:38:00, COWES, MEDINA ROAD COWES, 20, -21
# 📉 670719, 2022-11-16T22:21:00, LEE-ON-SOLENT, QUEENS ROAD LEE ON THE SOLENT, 51, -20
# 📉 667456, 2022-11-17T17:41:00, LANGSTONE HARBOUR, BUDDS FARM HAVANT NO2, 3401, -19
# 📉 667367, 2022-11-15T11:56:00, LANGSTONE HARBOUR, MAINLAND DRAYTON, 228, -19
# 📉 670897, 2022-11-16T22:07:00, SHEERNESS, SOUTH STREET QUEENBOROUGH, 57, -18
# 📉 667316, 2022-11-15T14:13:00, SEAGROVE, GREAT PRESTON ROAD RYDE, 382, -18
# 📉 671211, 2022-11-16T22:52:00, WORTHING, SOMPTING ROAD WORTHING, 15, -16
# 📉 670203, 2022-11-17T17:45:00, CHICHESTER HARBOUR, CHIDHAM, 1344, -15
# 📉 670266, 2022-11-16T21:00:00, LANCING BEACH GREEN, ROPETACKLE STREET SHOREHAM, 91, -15
# 📉 671928, 2022-11-17T07:01:00, RAMSGATE WESTERN UNDERCLIFFE, FOADS LANE RAMSGATE, 63, -14
# 📉 670180, 2022-11-17T01:01:00, PEVENSEY BAY, RATTLE ROAD WESTHAM, 381, -14
# 📉 670143, 2022-11-17T06:02:00, LITTLEHAMPTON, BROADMARK LANE RUSTINGTON, 672, -13
# 📉 670001, 2022-11-17T01:47:00, HILLHEAD, HOOK PARK NO.1, 443, -13
# 📉 670773, 2022-11-16T22:05:00, SHEERNESS, GRAIN NO2, 17, -13
# 📉 670613, 2022-11-16T22:05:00, COWES, MEDINA ROAD COWES, 44, -11
# 📉 670665, 2022-11-16T21:34:00, COWES, CASTLE STREET COWES, 14, -11
# 📉 670156, 2022-11-16T19:04:00, COWES, CASTLE STREET COWES, 4, -11
# 📉 672001, 2022-11-17T06:12:00, HERNE BAY CENTRAL, GAINSBOROUGH DRIVE HERNE BAY, 48, -9
# 📉 671188, 2022-11-17T02:07:00, RAMSGATE WESTERN UNDERCLIFFE, MILITARY ROAD RAMSGATE NO1, 82, -8
# 📉 670551, 2022-11-16T19:38:00, SALTDEAN, PORTOBELLO BRIGHTON NO1, 67, -7
# 📉 668963, 2022-11-16T08:53:00, CHICHESTER HARBOUR, CHICHESTER, 17, -7
# 📉 670206, 2022-11-16T20:15:00, ST HELENS, LATIMER ROAD ST HELENS, 45, -6
# 📉 672416, 2022-11-17T14:55:00, CHICHESTER HARBOUR, CHICHESTER, 19, -5
# 📉 671578, 2022-11-17T02:40:00, SALTDEAN, PORTOBELLO BRIGHTON NO2, 40, -5
# 📉 671473, 2022-11-17T03:44:00, RAMSGATE WESTERN UNDERCLIFFE, FOADS LANE RAMSGATE, 103, -4
# 📉 670730, 2022-11-16T22:26:00, COWES, EGYPT POINT COWES, 43, -4
# 📉 670742, 2022-11-16T21:44:00, COWES, CASTLE STREET COWES, 3, -3
# 📉 668831, 2022-11-16T08:28:00, CHICHESTER HARBOUR, CHICHESTER, 70, -2

# Deleted Events by duration
# --------------------------
# eventId, eventStop, bathingSite, outfallName, duration
# 👻 658534, 2022-11-17T18:00:01.973, HERNE BAY CENTRAL, KINGS HALL HERNE BAY A, 16185
# 👻 670100, 2022-11-17T04:00:00, FELPHAM, CHICHESTER ROAD BOGNOR, 585
# 👻 668629, 2022-11-16T10:30:00, SANDOWN, HOPE BEACH SHANKLIN NEW, 480
# 👻 672372, 2022-11-17T18:00:01.973, HERNE BAY CENTRAL, KINGS HALL HERNE BAY A, 270
# 👻 671876, 2022-11-17T07:15:59, HASTINGS PELHAM BEACH, CINQUE PORTS WAY HASTINGS NO1, 104
# 👻 669734, 2022-11-16T17:00:00, RYDE, AUGUSTA ROAD RYDE, 45
# 👻 672286, 2022-11-17T12:15:00, HASTINGS PELHAM BEACH, COOMBS HASTINGS, 45
# 👻 669200, 2022-11-16T10:45:00, CHICHESTER HARBOUR, CHICHESTER, 19
# 👻 669757, 2022-11-16T17:00:00, CHICHESTER HARBOUR, CHICHESTER, 18
# 👻 669171, 2022-11-16T10:15:00, CHICHESTER HARBOUR, CHICHESTER, 17
# 👻 669651, 2022-11-16T15:30:00, CHICHESTER HARBOUR, CHICHESTER, 17
# 👻 672418, 2022-11-17T15:00:00, HASTINGS PELHAM BEACH, COOMBS HASTINGS, 17
# 👻 669337, 2022-11-16T10:15:00, WEST BEACH WHITSTABLE, TANKERTON CIRCUS, 16
# 👻 669540, 2022-11-16T14:30:00, CHICHESTER HARBOUR, CHICHESTER, 16
# 👻 672436, 2022-11-17T15:46:55, HASTINGS PELHAM BEACH, COOMBS HASTINGS, 16
# 👻 669462, 2022-11-16T13:30:00, CHICHESTER HARBOUR, CHICHESTER, 15
# 👻 669512, 2022-11-16T14:00:00, CHICHESTER HARBOUR, CHICHESTER, 15
# 👻 672408, 2022-11-17T14:15:00, HASTINGS PELHAM BEACH, COOMBS HASTINGS, 15

# Events that were genuine but not any more by duration
# -----------------------------------------------------
# eventId, eventStop, bathingSite, outfallName, duration, activity
# 🤔 667364, 2022-11-24T10:00:01.827, CHICHESTER HARBOUR, THORNHAM, 13076, Not Genuine
# 🤔 667659, 2022-11-23T10:34:49, COLWELL BAY, THE PROMENADE TOTLAND PIER NO2, 11516, Not Genuine
# 🤔 670181, 2022-11-24T10:00:01.827, TANKERTON, GORREL NO1, 10983, Not Genuine
# ✅ Done

class DiscrepancyFinder:
    def __init__(self,input_files, dbcon=":memory:"):
        self.fields = [
            'recordId',
            'eventId',
            'siteUnitNumber',
            'bathingSite',
            'eventStart',
            'eventStop',
            'duration',
            'activity',
            'associatedSiteId',
            'outfallName',
            'isImpacting',
            'sourceFile'
        ]
        self.input_files = input_files
        
        self.connect_db(dbcon)
        self.create_schema()
        self.read_input_files(input_files)
        self.create_indexes()
        self.create_views()
        self.db.commit()

    def connect_db(self, dbcon):
        if dbcon != ':memory:' and os.path.exists(dbcon):
            os.remove(dbcon)
            print(f"Removed {dbcon} as it already existed")

        self.db = sqlite3.connect(dbcon)
        print(f"Creating db file {dbcon}")
        return self.db

    def close_db(self):
        self.db.commit()
        self.db.close()

    def create_schema(self):
        self.db.execute('create table records(recordId integer,eventId integer,siteUnitNumber integer,bathingSite,eventStart,eventStop,duration integer,activity,associatedSiteId integer,outfallName,isImpacting,sourceFile);')
    
    def create_indexes(self):
        self.db.execute('create index rec_recordId_idx on records(recordId);')
        self.db.execute('create index rec_eventId_idx on records(eventId);')

    def create_views(self):
        # View for the most recent record
        # pertaining to each event
        self.db.execute("""
            create view current_records as 
                select * 
                from records r1
                where recordId=(select max(r2.recordId)
                                from records r2
                                where r1.eventID=r2.eventId)
                order by eventId;
            """)

        # View for any records that
        # arent the most recent record
        # for an event
        self.db.execute("""
            create view old_records as 
                select * 
                from records r 
                where r.recordId not in (
                    select recordId from current_records c
                );
        """)

        # View for records sourced from the file
        # that contains the most recent record
        self.db.execute("""
            create view newest_file as
                select * from records r
                where r.sourceFile = (
                    select sourceFile from records r2
                    order by recordId desc 
                    limit 1
                );
        """)

    # Get events who's current duration is less than 
    # it was at some point in the past
    def get_duration_decreases(self):
        # How do I reuse this subquery?
        return self.db.execute("""
            select eventId, eventStop, bathingSite, outfallName,
            duration,
            duration -  (select max(duration)
                                from old_records o
                                where c.eventId = o.eventID) as decrease
            from current_records c
            where c.duration < (select max(duration)
                                from old_records o
                                where c.eventId = o.eventID)
            order by decrease;
        """)

    # Get events that exist in at least
    # one of the input files but not
    # the most recent file
    def get_deleted(self):
        return self.db.execute("""
            select eventId, eventStop, bathingSite, outfallName, duration 
            from records r
            where r.eventId not in (
                select eventId from newest_file
            )
            group by eventId
            order by r.duration desc;
        """)

    # Find events that were 'Genuine' but now aren't
    def get_ungenuine(self):
        return self.db.execute("""
            select eventId, eventStop, bathingSite, outfallName, duration, activity 
            from current_records c 
            where c.eventId in (
                select o.eventID
                from old_records o
                where o.activity in ('Genuine', 'Genuine - Non Impacting')
                intersect
                select c.eventID
                from current_records c 
                where c.activity not in ('Genuine', 'Genuine - Non Impacting')
            )
            order by duration desc;
        """)

    # Any more anomalies that should be detected?

    def insert_line(self, line, input_file):
        line_values = [v.strip() for v in line.split(",")] + [input_file]
        
        # Use a prepared statement just incase someone
        # manages to slip an sql injection into SW's data..
        self.db.execute(
            f"insert into records values({','.join(['?']*len(self.fields))});",
            line_values
        )

    def read_input_files(self, input_files):
        for input_file in input_files:
            with open(input_file) as file:
                lines = file.readlines()[1:]
                for line in lines:
                    self.insert_line(line, input_file)
                print(f"✅ Read {len(lines)} records from {input_file}")
                
    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input_files', nargs='*', 
        help='CSV files to read eg foo.csv bah.csv or *.csv')
    parser.add_argument('--sqlite-dbfile', default=":memory:", 
        help="Path to use for sqlite db, this file will be overwritten."
             " Default is to create it in memory")
    args = parser.parse_args()
    try:
        df = DiscrepancyFinder(input_files=args.input_files, dbcon=args.sqlite_dbfile)
        
        print("\nEvents with duration decreases by delta")
        print(  "---------------------------------------")
        print("eventId, eventStop, bathingSite, outfallName, current duration, duration decrease")
        for row in df.get_duration_decreases():
            fields = ", ".join(str(f) for f in row)
            print(f"📉 {fields}")

        print("\nDeleted Events by duration")
        print(  "--------------------------")
        print("eventId, eventStop, bathingSite, outfallName, duration")
        for row in df.get_deleted():
            fields = ", ".join(str(f) for f in row)
            print(f"👻 {fields}")


        print("\nEvents that were genuine but not any more by duration")
        print(  "-----------------------------------------------------")
        print("eventId, eventStop, bathingSite, outfallName, duration, activity")
        for row in df.get_ungenuine():
            fields = ", ".join(str(f) for f in row)
            print(f"🤔 {fields}")
    finally:
        if 'df' in locals():
            df.close_db()
    print("✅ Done")

if __name__ == "__main__":
    main()
