with base as (
    select *
    from `default`.`transactions`
),

exploded as (
    select
        UniqueId,
        TransactionDateUTC,
        OriginAirportCode,
        DestinationAirportCode,
        Itinerary,
        OneWayOrReturn,
        JSONExtractArrayRaw(ifNull(Segment, '[]')) as SegmentArray
    from base
),

segments as (
    select
        UniqueId,
        TransactionDateUTC,
        OriginAirportCode,
        DestinationAirportCode,
        Itinerary,
        OneWayOrReturn,
        JSONExtractString(segment, 'DepartureAirportCode') as DepartureAirportCode,
        JSONExtractString(segment, 'ArrivalAirportCode') as ArrivalAirportCode,
        toInt32OrNull(JSONExtractString(segment, 'SegmentNumber')) as SegmentNumber,
        toInt32OrNull(JSONExtractString(segment, 'LegNumber')) as LegNumber,
        toInt32OrNull(JSONExtractString(segment, 'NumberOfPassengers')) as NumberOfPassengers
    from exploded
    array join SegmentArray as segment
)

select * from segments