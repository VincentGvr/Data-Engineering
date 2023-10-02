--1. CREATE TABLE. NOTE THAT THE EMAIL AND USERPWD FIELDS MASKED
CREATE TABLE dbo.DDMExample 
(UserID	INT IDENTITY(1,1) PRIMARY KEY NONCLUSTERED NOT ENFORCED,
Firstname VARCHAR(40), 
Lastname VARCHAR(40), 
Username VARCHAR(40), 
UserLoginID bigint MASKED WITH (FUNCTION = 'random(50000, 75000)'), 
Email VARCHAR(50) MASKED WITH (FUNCTION = 'email()'), 
UserPwd VARCHAR(50) MASKED WITH (FUNCTION = 'default()')); 
GO


INSERT INTO dbo.DDMExample (Firstname, Lastname, Username, UserLoginID, Email, UserPwd) 
VALUES ('John','Smith','JSmith', 372036854775808, 'johnsmith@gmail.com','123456ABCDE');
 
INSERT INTO dbo.DDMExample (Firstname, Lastname, Username, UserLoginID, Email, UserPwd) 
VALUES ('Jane','Doe','JDoe', 372032254855106, 'janedoe@gmail.com','112233ZYXWV');
 
INSERT INTO dbo.DDMExample (Firstname, Lastname, Username, UserLoginID, Email, UserPwd) 
VALUES ('Walt','Disney','WDisney', 372031114679991, 'waltdisney@gmail.com','998877AZBYC'); 


--2. Create a test user
CREATE USER TestUser WITHOUT LOGIN; 
GO 
GRANT SELECT ON dbo.DDMExample TO TestUser; 
GO  


--3. Query the table as Test user - Queries executed the as the TestUser view masked data. 
EXECUTE AS USER = 'TestUser';   
SELECT * FROM dbo.DDMExample ;   
REVERT;    
GO 


--4. Remove masking on email column
ALTER TABLE dbo.DDMExample ALTER COLUMN [Email] DROP MASKED

--5. Run the select against after maksing has been removed from email and know you will be able to see complete email address

EXECUTE AS USER = 'TestUser';   
SELECT * FROM dbo.DDMExample ;   
REVERT;    
GO 

--6. Remove masking on Password column
ALTER TABLE dbo.DDMExample ALTER COLUMN [UserPwd] DROP MASKED

--7. Run the select against after masking has been removed from Password column and know you will be able to see complete the complete Pasword

EXECUTE AS USER = 'TestUser';   
SELECT * FROM dbo.DDMExample ;   
REVERT;    
GO 

-- 8.  Remove masking on UserID column
ALTER TABLE dbo.DDMExample ALTER COLUMN [UserLoginID] DROP MASKED

--9. Run the select against after masking has been removed from User ID column and know you will be able to see the complete USER ID
EXECUTE AS USER = 'TestUser';   
SELECT * FROM dbo.DDMExample ;   
REVERT;    
GO 


--10. Cleanup
DROP USER TestUser
DROP TABLE DDMExample




