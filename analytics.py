import pandas as pd
import sqlite3
import psycopg2 as ps


# SQLite portion#########################################################################

def get_averages_box_lite(lat1: float, long1: float, lat2: float, long2: float):
    # connecting to SQLite db, calculating the minimum and maximum of latitude and longitude before passing the variables to a query
    con = sqlite3.connect('jobsity.db', isolation_level=None)
    lat_low = min(lat1, lat2)
    lat_high = max(lat1, lat2)
    long_low = min(long1, long2)
    long_high = max(long1, long2)
    cur = con.cursor()
    sql = """
		with base as (
		select STRFTIME('%Y/%m/%d', datetime) as date, STRFTIME('%Y/%m/%d',DATE(datetime, cast(strftime('%w', datetime)*(-1) as varchar) || ' days') )  as start_of_week, count(datetime) as trips
		from trips
		where origin_lat BETWEEN """ + str(lat_low) + ' and ' + str(lat_high) + """ 
			and origin_long between """ + str(long_low) + ' and ' + str(long_high) + """ 
			and destination_lat between """ + str(lat_low) + ' and ' + str(lat_high) + """
			and destination_long between """ + str(long_low) + ' and ' + str(long_high) + """
		group by STRFTIME('%Y/%m/%d', datetime), STRFTIME('%Y/%m/%d',DATE(datetime, cast(strftime('%w', datetime)*(-1) as varchar) || ' days') ) 
		)
		select start_of_week, round(avg(trips),2) as average_trips
		from base
		group by start_of_week
		order by start_of_week desc
	"""
    res = cur.execute(sql)
    return dict(res.fetchall())


def get_averages_region_lite(region: str):
    # connecting to SQLite db and passing the variable to a query
    con = sqlite3.connect('jobsity.db', isolation_level=None)
    cur = con.cursor()
    sql = """
		with base as (
			select STRFTIME('%Y/%m/%d', datetime) as date, STRFTIME('%Y/%m/%d',DATE(datetime, cast(strftime('%w', datetime)*(-1) as varchar) || ' days') )  as start_of_week, count(datetime) as trips
			from trips
			where region = '""" + region + """'
			group by STRFTIME('%Y/%m/%d', datetime), STRFTIME('%Y/%m/%d',DATE(datetime, cast(strftime('%w', datetime)*(-1) as varchar) || ' days') ) 
			)
		select start_of_week, round(avg(trips),2) as average_trips
		from base
		group by start_of_week
		order by start_of_week desc
		"""
    res = cur.execute(sql)
    return dict(res.fetchall())


def get_averages_region_box_lite(lat1: float, long1: float, lat2: float, long2: float, region: str):
    # connecting to SQLite db, calculating the minimum and maximum of latitude and longitude before passing all the
    # variables to a query
    con = sqlite3.connect('jobsity.db', isolation_level=None)
    lat_low = min(lat1, lat2)
    lat_high = max(lat1, lat2)
    long_low = min(long1, long2)
    long_high = max(long1, long2)
    cur = con.cursor()
    sql = """
		with base as (
		select STRFTIME('%Y/%m/%d', datetime) as date, STRFTIME('%Y/%m/%d',DATE(datetime, cast(strftime('%w', datetime)*(-1) as varchar) || ' days') )  as start_of_week, count(datetime) as trips
		from trips
		where origin_lat BETWEEN """ + str(lat_low) + ' and ' + str(lat_high) + """ 
			and origin_long between """ + str(long_low) + ' and ' + str(long_high) + """ 
			and destination_lat between """ + str(lat_low) + ' and ' + str(lat_high) + """
			and destination_long between """ + str(long_low) + ' and ' + str(long_high) + """
			and region = '""" + region + """'
		group by STRFTIME('%Y/%m/%d', datetime), STRFTIME('%Y/%m/%d',DATE(datetime, cast(strftime('%w', datetime)*(-1) as varchar) || ' days') ) 
		)
		select start_of_week, round(avg(trips),2) as average_trips
		from base
		group by start_of_week
		order by start_of_week desc
	"""
    res = cur.execute(sql)
    return dict(res.fetchall())


# AWS portion#########################################################################

def get_averages_box_aws(lat1: float, long1: float, lat2: float, long2: float):
    # connecting to Redshift, calculating the minimum and maximum of latitude and longitude before passing the
    # variables to a query
    ps_con = ps.connect(dbname='trips',
                        host='prod.triptripping.us-east-x.redshift.amazonaws.com',
                        port='9000',
                        user='usr',
                        password='superSecurePassword')
    cur = ps_con.cursor()

    lat_low = min(lat1, lat2)
    lat_high = max(lat1, lat2)
    long_low = min(long1, long2)
    long_high = max(long1, long2)
    sql = """
		with base as (
			select cast(datetime as date) as date, dateadd(d, -datepart(dow, datetime), cast(datetime as date))  as start_of_week, count(datetime) as trips
			from trips
			where origin_lat BETWEEN """ + str(lat_low) + ' and ' + str(lat_high) + """ 
				and origin_long between """ + str(long_low) + ' and ' + str(long_high) + """ 
				and destination_lat between """ + str(lat_low) + ' and ' + str(lat_high) + """
				and destination_long between """ + str(long_low) + ' and ' + str(long_high) + """
			group by select cast(datetime as date), dateadd(d, -datepart(dow, datetime), cast(datetime as date)) 
		)
		select start_of_week, round(avg(trips),2) as average_trips
		from base
		group by start_of_week
		order by start_of_week desc
	"""
    res = dict(cur.execute(sql).fetchall())
    ps_con.close()
    return res


def get_averages_region_aws(region: str):
    # connecting to Redshift and passing the variable to a query
    ps_con = ps.connect(dbname='trips',
                        host='prod.triptripping.us-east-x.redshift.amazonaws.com',
                        port='9000',
                        user='usr',
                        password='superSecurePassword')
    cur = ps_con.cursor()

    sql = """
		with base as (
			select cast(datetime as date) as date, dateadd(d, -datepart(dow, datetime), cast(datetime as date))  as start_of_week, count(datetime) as trips
			from trips
			where region = '""" + region + """'
			group by select cast(datetime as date), dateadd(d, -datepart(dow, datetime), cast(datetime as date)) 
			)
		select start_of_week, round(avg(trips),2) as average_trips
		from base
		group by start_of_week
		order by start_of_week desc
		"""
    res = dict(cur.execute(sql).fetchall())
    ps_con.close()
    return res


def get_averages_region_box_aws(lat1: float, long1: float, lat2: float, long2: float, region: str):
    # connecting to Redshift, calculating the minimum and maximum of latitude and longitude before passing all the variables to a query
    ps_con = ps.connect(dbname='trips',
                        host='prod.triptripping.us-east-x.redshift.amazonaws.com',
                        port='9000',
                        user='usr',
                        password='superSecurePassword')
    cur = ps_con.cursor()

    lat_low = min(lat1, lat2)
    lat_high = max(lat1, lat2)
    long_low = min(long1, long2)
    long_high = max(long1, long2)
    sql = """
		with base as (
			select cast(datetime as date) as date, dateadd(d, -datepart(dow, datetime), cast(datetime as date))  as start_of_week, count(datetime) as trips
			from trips
			where origin_lat BETWEEN """ + str(lat_low) + ' and ' + str(lat_high) + """ 
				and origin_long between """ + str(long_low) + ' and ' + str(long_high) + """ 
				and destination_lat between """ + str(lat_low) + ' and ' + str(lat_high) + """
				and destination_long between """ + str(long_low) + ' and ' + str(long_high) + """
				and region = '""" + region + """'
			group by select cast(datetime as date), dateadd(d, -datepart(dow, datetime), cast(datetime as date)) 
		)
		select start_of_week, round(avg(trips),2) as average_trips
		from base
		group by start_of_week
		order by start_of_week desc
	"""
    res = dict(cur.execute(sql).fetchall())
    ps_con.close()
    return res
