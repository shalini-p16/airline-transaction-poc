with base as (
    select *
    from {{ ref('int_transactions_enriched') }}
),

transaction_summary as (
    select
        OriginCountry,
        count(distinct UniqueId) as TransactionCount,
        countDistinctIf(UniqueId, JourneyType = 'Domestic') as DomesticTransactions,
        countDistinctIf(UniqueId, JourneyType = 'International') as InternationalTransactions,
        avg(NumberOfSegments) as AverageSegmentsPerTransaction
    from base
    group by OriginCountry
)

select * from transaction_summary
