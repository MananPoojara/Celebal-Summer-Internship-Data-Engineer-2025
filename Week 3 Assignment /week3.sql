-- Task 1 Query all columns (attributes) for every row in the CITY table. The CITY table is described as follows:
SELECT * FROM CITY;


-- Task 2 Query all columns for a city in CITY with the ID 1661.
SELECT * FROM CITY WHERE ID = 1661; 


-- Task 3 Write a query that prints a list of employee names (i.e.: the name attribute) from the Employee table in alphabetical order.
SELECT name FROM Employee ORDER BY name ASC;

--Task 4 Query all attributes of every Japanese city in the CITY table. The COUNTRYCODE for Japan is JPN.
SELECT * FROM CITY where COUNTRYCODE = 'JPN';


--Task 5 Query a list of CITY and STATE from the STATION table.
SELECT CITY, STATE FROM STATION;

--Task 6 Query a list of CITY names from STATION for cities that have an even ID number. Print the results in any order, but exclude duplicates from the answer.
SELECT DISTINCT CITY FROM STATION where ID % 2 = 0;


--Task 7 Find the difference between the total number of CITY entries in the table and the number of distinct CITY entries in the table.
SELECT COUNT(CITY) - COUNT(DISTINCT CITY) FROM STATION;


--Task 8 Query the two cities in STATION with the shortest and longest CITY names, as well as their respective lengths (i.e.: number of characters in the name). If there is more than one smallest or largest city, choose the one that comes first when ordered alphabetically.

(
    SELECT CITY, LENGTH(CITY) AS NAME_LENGTH FROM STATION ORDER BY LENGTH(CITY) ASC, CITY ASC 
    LIMIT 1
)
UNION ALL 
(
    SELECT CITY, LENGTH(CITY) AS NAME_LENGTH FROM STATION ORDER BY LENGTH(CITY) DESC, CITY DESC
    LIMIT 1
);


--Task 9 Query the average population for all cities in CITY, rounded down to the nearest integer.
SELECT ROUND(AVG(POPULATION)) FROM CITY;


--Task 10 Given the CITY and COUNTRY tables, query the names of all the continents (COUNTRY.Continent) and their respective average city populations (CITY.Population) rounded down to the nearest integer.
SELECT COUNTRY.Continent, FLOOR(AVG(CITY.Population)) FROM CITY JOIN COUNTRY ON CITY.CountryCode = COUNTRY.Code GROUP BY COUNTRY.Continent;
