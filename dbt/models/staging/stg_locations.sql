select
    AirportCode,
    CountryName,
    Region
from {{ source('default', 'locations') }}
