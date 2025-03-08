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
        , [LastDate] = DateFromParts(year(getdate())+1,12,31)
    )
,Dates as (
    select 
        [Date] = Dateadd(day,n.n-1, dr.FirstDate)
    FROM DateRange dr 
        cross join numbers n
    WHERE N <= DateDiff(day, dr.FirstDate, dr.LastDate)+1
    )
SELECT 
    d.[Date]
    , [Year] = Year(d.[Date])
    , [Quarter] = concat('Q',DatePart(quarter,d.[Date]))
    , [YearQuarter] = concat(Year(d.[Date]),'-Q', DatePart(quarter,d.[Date]))
    , [MonthNum] = Month(d.[Date])
    , [Month] = DateName(Month, d.[Date])
    , [YearMonth] = concat(Year(d.[Date]),'-', right(concat('0', Month(d.[Date])), 2))
    , [DayNumber] = Day(d.[Date])
    , [WeekdayNum] = DatePart(Weekday,d.[Date])
    , [Weekday] = DateName(Weekday,d.[Date])
    , [IsWeekday] = case when DatePart(Weekday,d.[Date]) between 2 and 6 then 1 else 0 end
    
from dates d


