drop table shapes;
drop table routes;
drop table trips;
drop table stops;
drop table stop_times;

CREATE TABLE shapes(
    id int NOT NULL,
    latitude decimal(10,8),
    longitude decimal(11,8),
    number int NOT NULL,
    distance decimal(6,4),
    PRIMARY KEY(id, number)
);

CREATE TABLE routes(
    route_long_name varchar(64),
    route_color varchar(8),
    route_id int NOT NULL,
    PRIMARY KEY(route_id)
);

CREATE TABLE trips(
   
   block_id varchar(16),
   route_id int NOT NULL,
   direction_id int,
   trip_headsign varchar(64),
   shape_id int,
   service_id varchar(32),
   trip_id int NOT NULL,
   PRIMARY KEY(route_id,trip_id)
);

CREATE TABLE stops(
   stop_lat decimal(10,8),
   stop_code int,
   stop_lon decimal(11,8),
   stop_desc varchar(32),
   stop_name varchar(32),
   stop_id int NOT NULL,  
   PRIMARY KEY(stop_id)
);

CREATE TABLE stop_times(
    trip_id int NOT NULL,
    arrival_time time,
    departure_time time,
    stop_id int NOT NULL,
    stop_sequence int,
    PRIMARY KEY(trip_id,stop_id,stop_sequence)
);

