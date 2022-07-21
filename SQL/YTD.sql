

      create or replace transient table milestone as
      (

select cast(getdate() as date) as ends_nav,(select n1."nav" from NAVHISTORY n1
where n1."nav_date"=datefromparts(year(n1."nav_date"),02,06)
and n1."code"=n2."code") as end_nav,
(select n1."nav" from NAVHISTORY n1
where n1."nav_date"=datefromparts(year(n1."nav_date"),01,01)
and n1."code"=n2."code") as start_nav,
((end_nav-start_nav)/start_nav*100) as ytd
from NAVHISTORY n2
order by n2."nav_date" asc
      );
    
