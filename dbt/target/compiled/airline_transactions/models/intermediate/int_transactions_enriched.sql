with base as (
    select *
    from `default`.`stg_transactions`
),

origins as (
    select
        base.*,
        loc.CountryName as OriginCountry
    from base
    left join `default`.`stg_locations` loc
    on base.OriginAirportCode = loc.AirportCode
),

destinations as (
    select
        origins.*,
        loc.CountryName as DestinationCountry
    from origins
    left join `default`.`stg_locations` loc
    on origins.DestinationAirportCode = loc.AirportCode
),

final as (
    select
        *,
        countDistinct(DepartureAirportCode) over (partition by UniqueId) as NumberOfSegments,
        if(OriginCountry = DestinationCountry, 'Domestic', 'International') as JourneyType
    from destinations
)

select distinct * from final