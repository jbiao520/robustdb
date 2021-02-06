CREATE TABLE Persons ( PersonID int(8) PRIMARY KEY, LastName varchar(255) not null, FirstName varchar(255), Address varchar(255), City varchar(255) );

ALTER table Persons ADD index idx_city(City);

INSERT into Persons(PersonID,LastName,FirstName,Address,City) values (1,'Guo','Jianbiao','Sunqiao','Shanghai');

SELECT LastName, FirstName, Address, City FROM Persons a where City='Shanghai';

update Persons set Address='Dahua' where PersonID=1;