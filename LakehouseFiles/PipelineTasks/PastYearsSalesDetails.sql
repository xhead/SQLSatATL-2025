Select 
    d.SalesOrderID
    , d.SalesOrderDetailId
    , d.ProductID
    , d.OrderQty
    , d.UnitPrice
    , d.UnitPriceDiscount
    , GrossSalesAmount = d.OrderQty * d.UnitPrice
    , DiscountAmount = d.OrderQty * d.UnitPriceDiscount
    , NetSalesAmount = (d.OrderQty * d.UnitPrice) - (d.OrderQty * d.UnitPriceDiscount)
from SalesLT.SalesOrderDetail d
	join SalesLT.SalesOrderHeader s on d.SalesOrderID = s.SalesOrderID
where s.OrderDate < DATEFROMPARTS(year(getdate()),1,1)
