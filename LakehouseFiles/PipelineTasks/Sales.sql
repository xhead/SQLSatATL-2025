select 
    s.SalesOrderID
    , s.CustomerID
    , s.RevisionNumber
    , s.OrderDate
    , s.DueDate
    , s.ShipDate
    , s.Status
    , PurchasedAt = case s.OnlineOrderFlag when 1 then 'Web' else 'Store' end 
    , s.PurchaseOrderNumber
    , s.AccountNumber
    , s.ShipMethod
    , TaxAmount = s.TaxAmt
    , s.Freight
from SalesLT.SalesOrderHeader s 