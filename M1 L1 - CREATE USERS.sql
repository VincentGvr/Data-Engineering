-- AUTHENTICATION
CREATE USER fabien FROM LOGIN [fabien.adato@microsoft.com];

-- or
CREATE USER [fabien.adato@microsoft.com] FROM EXTERNAL PROVIDER;

-- Connect to master database and create a login
CREATE LOGIN SQLDWLogin WITH PASSWORD = 'Str0ng_password';
CREATE USER SQLDWuser FOR LOGIN SQLDWLogin;

-- Connect to the user database(DW) and create a database user
CREATE USER SQLDWuser FOR LOGIN SQLDWLogin;


-- AUTHORIZATION
-- Allow SQLDWuser to read data
EXEC sp_addrolemember 'db_datareader', 'SQLDWuser'; 

-- Allow SQLDWuser to write data
EXEC sp_addrolemember 'db_datawriter', 'SQLDWuser';
