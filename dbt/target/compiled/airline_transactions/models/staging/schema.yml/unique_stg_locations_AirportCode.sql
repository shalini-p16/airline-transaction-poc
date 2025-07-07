
    
    

select
    AirportCode as unique_field,
    count(*) as n_records

from `default`.`stg_locations`
where AirportCode is not null
group by AirportCode
having count(*) > 1


