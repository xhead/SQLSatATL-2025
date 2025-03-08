begin tran

;WITH E00(N) AS (SELECT 1 UNION ALL SELECT 1)
    ,E02(N) AS (SELECT 1 FROM E00 a, E00 b)
    ,E04(N) AS (SELECT 1 FROM E02 a, E02 b)
    ,E08(N) AS (SELECT 1 FROM E04 a, E04 b)
    ,E16(N) AS (SELECT 1 FROM E08 a, E08 b)
    ,E32(N) AS (SELECT 1 FROM E16 a, E16 b)
    ,numbers(N) AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM E32)
    
,DateRange as (
    select
        [FirstDate] = DateFromParts(year(getdate())-1,1,1)
        , [LastDate] = DateFromParts(2025,3,15)
    )
,Dates as (
    select 
        [Date] = Dateadd(day,n.n-1, dr.FirstDate),
		Offset = n
    FROM DateRange dr 
        cross join numbers n
    WHERE N <= DateDiff(day, dr.FirstDate, dr.LastDate)+1
    )
INSERT INTO [SalesLT].[SalesOrderHeader]
           ([SalesOrderID]
           ,[RevisionNumber]
           ,[OrderDate]
           ,[DueDate]
           ,[ShipDate]
           ,[Status]
           ,[OnlineOrderFlag]
           ,[AccountNumber]
           ,[CustomerID]
           ,[ShipToAddressID]
           ,[BillToAddressID]
           ,[ShipMethod]
           ,[CreditCardApprovalCode]
           ,[SubTotal]
           ,[TaxAmt]
           ,[Freight]
           ,[Comment]
		   )
	select 
           s.SalesOrderID * 1000 + d.Offset
           ,s.RevisionNumber
           ,d.Date 
           ,dateadd(day, datediff(day, s.orderDate, s.DueDate) , d.Date)
           ,dateadd(day, datediff(day, s.orderDate, s.ShipDate), d.Date)
           ,s.Status
           ,s.OnlineOrderFlag
           ,s.AccountNumber
           ,s.CustomerID
           ,s.ShipToAddressID
           ,s.BillToAddressID
           ,s.ShipMethod
           ,s.CreditCardApprovalCode
           ,s.SubTotal
           ,s.TaxAmt
           ,s.Freight
           ,s.Comment
	from SalesLT.SalesOrderHeader s
		cross join dates d 
	where s.OrderDate < '2024-1-1'
		and  s.SalesOrderID * 1000 + d.Offset not in (select SalesOrderID from SalesLT.SalesOrderHeader)
;

WITH E00(N) AS (SELECT 1 UNION ALL SELECT 1)
    ,E02(N) AS (SELECT 1 FROM E00 a, E00 b)
    ,E04(N) AS (SELECT 1 FROM E02 a, E02 b)
    ,E08(N) AS (SELECT 1 FROM E04 a, E04 b)
    ,E16(N) AS (SELECT 1 FROM E08 a, E08 b)
    ,E32(N) AS (SELECT 1 FROM E16 a, E16 b)
    ,numbers(N) AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM E32)
    
,DateRange as (
    select
        [FirstDate] = DateFromParts(year(getdate())-1,1,1)
        , [LastDate] = DateFromParts(2025,3,15)
    )
,Dates as (
    select 
        [Date] = Dateadd(day,n.n-1, dr.FirstDate),
		Offset = n
    FROM DateRange dr 
        cross join numbers n
    WHERE N <= DateDiff(day, dr.FirstDate, dr.LastDate)+1
    )


INSERT INTO [SalesLT].[SalesOrderDetail]
           ([SalesOrderID]
           ,[OrderQty]
           ,[ProductID]
           ,[UnitPrice]
           ,[UnitPriceDiscount]
			)
SELECT s.SalesOrderID * 1000 + d.Offset
      ,s.[OrderQty]
      ,s.[ProductID]
      ,s.[UnitPrice]
      ,s.[UnitPriceDiscount]
  FROM [SalesLT].[SalesOrderDetail] s
	cross join dates d 
where s.SalesOrderID <= 71946
	and  s.SalesOrderID * 1000 + d.Offset not in (select SalesOrderID from SalesLT.SalesOrderDetail)	

-- rollback
commit

---- remove sample rows to cleanup if need to redo sample data
--delete from SalesLT.SalesOrderDetail where SalesOrderID > 71946
--delete from SalesLT.SalesOrderHeader where SalesOrderID > 71946

