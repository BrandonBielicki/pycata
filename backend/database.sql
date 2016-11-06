CREATE TABLE shapes(
    id int NOT NULL,
    latitude decimal(10,8),
    longitude decimal(11,8),
    number int NOT NULL,
    distance decimal(6,4),
    PRIMARY KEY(id, number)
);

CREATE TABLE routes(
    id int NOT NULL,
    name varchar(64),
    color varchar(8),
    PRIMARY KEY(id)
);

CREATE TABLE trips(
    route_id int NOT NULL,
    service_id int,
    trip_id int NOT NULL,
    head_sign varchar(64),
    direction int,
    block_id int,
    shape_id int,
    PRIMARY KEY(route_id,trip_id)
);

CREATE TABLE stops(
   id int NOT NULL,
   code varchar(32),
   name varchar(32),
   description varchar(64),
   latitude decimal(10,8),
   longitude decimal(11,8),
   PRIMARY KEY(id)
);

CREATE TABLE stop_times(
    trip_id int NOT NULL,
    arrival time,
    departure time,
    stop_id int NOT NULL,
    stop_sequence int,
    PRIMARY KEY(trip_id,stop_id,stop_sequence)
);

