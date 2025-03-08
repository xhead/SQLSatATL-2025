with a as (
select ca.CustomerID
    , a.AddressLine1
    , a.AddressLine2
    , a.City
    , a.StateProvince
    , a.CountryRegion
    , a.PostalCode
from SalesLT.CustomerAddress ca
    join SalesLT.Address a on ca.AddressID = a.AddressID and ca.AddressType = 'Main Office'
)
select c.CustomerID
    , c.NameStyle
    , c.Title
    , c.FirstName
    , c.MiddleName
    , c.LastName
    , c.suffix
    , c.CompanyName
    , c.SalesPerson
    , c.EmailAddress
    , c.Phone
    , a.AddressLine1
    , a.AddressLine2
    , a.City
    , a.StateProvince
    , a.CountryRegion
    , a.PostalCode
from SalesLT.Customer c 
    left join a on c.CustomerID = a.CustomerID
