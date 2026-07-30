CREATE TABLE IF NOT EXISTS city (
    cityId SERIAL PRIMARY KEY,
    cityName VARCHAR(100) NOT NULL,
    stateName VARCHAR(100),
    countryName VARCHAR(100),
    latitude FLOAT8,
    longtitude FLOAT8, 
    CONSTRAINT unique_city_country UNIQUE(cityName, countryName)
);

CREATE TABLE IF NOT EXISTS pollution (
    cityId INT,
    datetime TIMESTAMP,
    ts VARCHAR(50), 
    aqius INT,
    mainus VARCHAR(10),
    aqicn INT,
    maincn VARCHAR(10), 
    PRIMARY KEY (cityId, datetime),
    FOREIGN KEY (cityId) REFERENCES city(cityId) ON DELETE CASCADE
);