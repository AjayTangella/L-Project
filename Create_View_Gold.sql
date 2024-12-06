------------------------
-- CREATE VIEW CALENDAR
------------------------
CREATE VIEW gold.calendar
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://awsprojectdatalake.blob.core.windows.net/silver/Calender/',
            FORMAT = 'delta'
        ) as QUER1


------------------------
-- CREATE VIEW CUSTOMERS
------------------------
CREATE VIEW gold.customers
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://awsprojectdatalake.blob.core.windows.net/silver/Customers/',
            FORMAT = 'delta'
        ) as QUER1



------------------------
-- CREATE VIEW PRODUCTS
------------------------
CREATE VIEW gold.products
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://awsprojectdatalake.blob.core.windows.net/silver/Products/',
            FORMAT = 'delta'
        ) as QUER1


------------------------
-- CREATE VIEW RETURNS
------------------------
CREATE VIEW gold.returns
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://awsprojectdatalake.blob.core.windows.net/silver/Returns/',
            FORMAT = 'delta'
        ) as QUER1
        

------------------------
-- CREATE VIEW RETURNS
------------------------
CREATE VIEW gold.returns
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://awsprojectdatalake.blob.core.windows.net/silver/Returns/',
            FORMAT = 'delta'
        ) as QUER1


    ------------------------
-- CREATE VIEW SALES
------------------------
CREATE VIEW gold.sales
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://awsprojectdatalake.blob.core.windows.net/silver/Sales/',
            FORMAT = 'delta'
        ) as QUER1


------------------------
-- CREATE VIEW SUBCAT
------------------------
CREATE VIEW gold.subcat
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://awsprojectdatalake.blob.core.windows.net/silver/SUbCategories/',
            FORMAT = 'delta'
        ) as QUER1



------------------------
-- CREATE VIEW TERRITORIES
------------------------
CREATE VIEW gold.territories
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://awsprojectdatalake.blob.core.windows.net/silver/Territories/',
            FORMAT = 'delta'
        ) as QUER1

