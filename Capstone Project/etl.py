#!/usr/bin/env python3

import apache_beam as beam
import csv

DATETIME_FORMAT='%Y-%m-%dT%H:%M:%S'

def addtimezone(lat, lon):
    '''
        Uses the timezonefinder library to attach a timezone to every airport
    '''
    try:
        import timezonefinder
        tf = timezonefinder.TimezoneFinder()
        return (lat, lon, tf.timezone_at(lng=float(lon), lat=float(lat)))
        #return (lat, lon, 'America/Los_Angeles') # FIXME
    except ValueError:
        return (lat, lon, 'TIMEZONE') # header

def as_utc(date, hhmm, tzone):
    '''
        Returns offset compared to UTC timezone 
    '''
    try:
        if len(hhmm) > 0 and tzone is not None:
            import datetime, pytz
            loc_tz = pytz.timezone(tzone)
            loc_dt = loc_tz.localize(datetime.datetime.strptime(date,'%Y-%m-%d'), is_dst=False)
            # can't just parse hhmm because the data contains 2400 and the like ...
            loc_dt += datetime.timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
            utc_dt = loc_dt.astimezone(pytz.utc)
            return utc_dt.strftime(DATETIME_FORMAT), loc_dt.utcoffset().total_seconds()
        else:
            return '',0 # empty string corresponds to canceled flights
    except ValueError as e:
        print ('{} {} {}'.format(date, hhmm, tzone))
        raise e

def add_24h_if_before(arrtime, deptime):
    '''
        Check for potential issues with date of departure and arrival and fixes them
    '''
    import datetime
    if len(arrtime) > 0 and len(deptime) > 0 and (arrtime < deptime):
        adt = datetime.datetime.strptime(arrtime, DATETIME_FORMAT)
        adt += datetime.timedelta(hours=24)
        return adt.strftime(DATETIME_FORMAT)
    else:
        return arrtime

def tz_correct(line, airport_timezones_dict):
    '''
        Converts every flight record time to UTC to enforce consistency in the table
    '''
    def airport_timezone(airport_id):
        if airport_id in airport_timezones_dict:
            return airport_timezones_dict[airport_id]
        else:
            return ('37.52', '-92.17', u'America/Chicago') # population center of US
    fields = line.split(',')
    if fields[0] != 'FL_DATE' and len(fields) == 27:
        # convert all times to UTC
        dep_airport_id = fields[6]
        arr_airport_id = fields[10]
        dep_timezone = airport_timezone(dep_airport_id)[2] 
        arr_timezone = airport_timezone(arr_airport_id)[2]
      
        for f in [13, 14, 17]: #crsdeptime, deptime, wheelsoff
            fields[f], deptz = as_utc(fields[0], fields[f], dep_timezone)
        for f in [18, 20, 21]: #wheelson, crsarrtime, arrtime
            fields[f], arrtz = as_utc(fields[0], fields[f], arr_timezone)
      
        for f in [17, 18, 20, 21]:
            fields[f] = add_24h_if_before(fields[f], fields[14])

        fields.extend(airport_timezone(dep_airport_id))
        fields[-1] = str(deptz)
        fields.extend(airport_timezone(arr_airport_id))
        fields[-1] = str(arrtz)

        yield fields

def get_next_event(fields):
    '''
        Fills the events columns
    '''
    if len(fields[14]) > 0:
        event = list(fields) # copy
        event.extend(['departed', fields[14]])
        for f in [16,17,18,19,21,22,25]:
            event[f] = ''  # not knowable at departure time
        yield event
    if len(fields[17]) > 0:
        event = list(fields) # copy
        event.extend(['wheelsoff', fields[17]])
        for f in [18,19,21,22,25]:
            event[f] = ''  # not knowable at wheelsoff time
        yield event
    if len(fields[21]) > 0:
        event = list(fields)
        event.extend(['arrived', fields[21]])
        yield event

def create_row(fields):
    header = 'FL_DATE,UNIQUE_CARRIER,AIRLINE_ID,CARRIER,FL_NUM,ORIGIN_AIRPORT_ID,ORIGIN_AIRPORT_SEQ_ID,ORIGIN_CITY_MARKET_ID,ORIGIN,DEST_AIRPORT_ID,DEST_AIRPORT_SEQ_ID,DEST_CITY_MARKET_ID,DEST,CRS_DEP_TIME,DEP_TIME,DEP_DELAY,TAXI_OUT,WHEELS_OFF,WHEELS_ON,TAXI_IN,CRS_ARR_TIME,ARR_TIME,ARR_DELAY,CANCELLED,CANCELLATION_CODE,DIVERTED,DISTANCE,DEP_AIRPORT_LAT,DEP_AIRPORT_LON,DEP_AIRPORT_TZOFFSET,ARR_AIRPORT_LAT,ARR_AIRPORT_LON,ARR_AIRPORT_TZOFFSET,EVENT,NOTIFY_TIME'.split(',')

    featdict = {}
    for name, value in zip(header, fields):
        featdict[name] = value
    featdict['EVENT_DATA'] = ','.join(fields)
    return featdict
 
def run(project, bucket, dataset):
    '''
        This function describes the etl pipeline
    '''
    argv = [
        '--project={0}'.format(project),
        '--job_name=ch04timecorr',
        '--save_main_session',
        '--staging_location=gs://{0}/flights/staging/'.format(bucket),
        '--temp_location=gs://{0}/flights/temp/'.format(bucket),
        '--setup_file=./setup.py',
        '--max_num_workers=8',
        '--autoscaling_algorithm=THROUGHPUT_BASED',
        '--runner=DataflowRunner'
    ]
    airports_filename = 'gs://{}/flights/airports/airports.csv.gz'.format(bucket)
    flights_raw_files = 'gs://{}/flights/raw/*.csv'.format(bucket)
    flights_output = 'gs://{}/flights/tzcorr/all_flights'.format(bucket)
    events_output = '{}:{}.simevents'.format(project, dataset)

    pipeline = beam.Pipeline(argv=argv)
    
    # this section adds timezones to the airport information
    airports = (pipeline 
        | 'airports:read' >> beam.io.ReadFromText(airports_filename)
        | 'airports:fields' >> beam.Map(lambda line: next(csv.reader([line])))
        | 'airports:tz' >> beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
    )
    
    # unites airports and flights datasets, enforces common timezone 
    flights = (pipeline 
        | 'flights:read' >> beam.io.ReadFromText (flights_raw_files)
        | 'flights:tzcorr' >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
    )
    
    # keeps the results while processing the events
    (flights 
        | 'flights:tostring' >> beam.Map(lambda fields: ','.join(fields)) 
        | 'flights:out' >> beam.io.textio.WriteToText(flights_output)
    )
    
    # creates the events
    events = flights | beam.FlatMap(get_next_event)

    schema = 'FL_DATE:date,UNIQUE_CARRIER:string,AIRLINE_ID:string,CARRIER:string,FL_NUM:string,ORIGIN_AIRPORT_ID:string,ORIGIN_AIRPORT_SEQ_ID:integer,ORIGIN_CITY_MARKET_ID:string,ORIGIN:string,DEST_AIRPORT_ID:string,DEST_AIRPORT_SEQ_ID:integer,DEST_CITY_MARKET_ID:string,DEST:string,CRS_DEP_TIME:timestamp,DEP_TIME:timestamp,DEP_DELAY:float,TAXI_OUT:float,WHEELS_OFF:timestamp,WHEELS_ON:timestamp,TAXI_IN:float,CRS_ARR_TIME:timestamp,ARR_TIME:timestamp,ARR_DELAY:float,CANCELLED:string,CANCELLATION_CODE:string,DIVERTED:string,DISTANCE:float,DEP_AIRPORT_LAT:float,DEP_AIRPORT_LON:float,DEP_AIRPORT_TZOFFSET:float,ARR_AIRPORT_LAT:float,ARR_AIRPORT_LON:float,ARR_AIRPORT_TZOFFSET:float,EVENT:string,NOTIFY_TIME:timestamp,EVENT_DATA:string'
    
    # clears destination table and writes rows
    (events 
        | 'events:totablerow' >> beam.Map(lambda fields: create_row(fields)) 
        | 'events:out' >> beam.io.WriteToBigQuery(
                              events_output, schema=schema,
                              write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )

    pipeline.run()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Run pipeline on the cloud')
    parser.add_argument('-p','--project', help='Unique project ID', required=True)
    parser.add_argument('-b','--bucket', help='Bucket where your data were ingested', required=True)
    parser.add_argument('-d','--dataset', help='BigQuery dataset', default='flights')
    args = vars(parser.parse_args())

    print ("Correcting timestamps and writing to BigQuery dataset {}".format(args['dataset']))
  
    run(project=args['project'], bucket=args['bucket'], dataset=args['dataset'])
