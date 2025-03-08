SELECT p.ProductID
    , p.Name as Product
    , pm.Name as Model
    , pc.ParentProductCategoryName as Category
    , pc.ProductCategoryName as Subcategory
    , d.Description
    , p.Color
    , p.StandardCost
    , p.ListPrice
    , p.Size
    , p.Weight
    , p.SellStartDate
    , p.SellEndDate
    , p.DiscontinuedDate

from SalesLT.Product p 
    join SalesLT.vGetAllCategories pc on p.ProductCategoryID = pc.ProductCategoryID
    join SalesLT.ProductModel pm on p.ProductModelID  = pm.productModelID
    left join SalesLT.vProductAndDescription d on p.ProductID = d.ProductID and d.Culture = 'en'