drop table if exists `covid_273.oxford_policy_tracker_523`;
create or replace table `covid_273.oxford_policy_tracker_523` 
PARTITION BY
  `date`
  OPTIONS (
    partition_expiration_days = 720)
AS (select * from `bigquery-public-data.covid19_govt_response.oxford_policy_tracker` where alpha_3_code not in unnest(['GBR', 'BRA', 'CAN', 'USA'])) ;

alter table `covid_273.oxford_policy_tracker_523` 
add column population INTEGER, 
add column country_area FLOAT64,
add column mobility 
STRUCT< avg_retail FLOAT64,
        avg_grocery FLOAT64,
        avg_parks FLOAT64,
        avg_transit FLOAT64,
        avg_workplace FLOAT64,
        avg_residential FLOAT64>;

update `covid_273.oxford_policy_tracker_523` t0 set t0.`population` = t2.pop_data_2019 from (select distinct alpha_3_code, country_name, `date` from `covid_273.oxford_policy_tracker_523`) t1 left join (select DISTINCT country_territory_code, pop_data_2019 from `bigquery-public-data.covid19_ecdc.covid_19_geographic_distribution_worldwide`) as t2 on t1.alpha_3_code=t2.country_territory_code where CONCAT(t0.country_name, t0.date) = CONCAT(t1.country_name, t1.date);

update `covid_273.oxford_policy_tracker_523` t0 set t0.country_area = t1.country_area from `bigquery-public-data.census_bureau_international.country_names_area` t1 where t0.country_name = t1.country_name;

update `covid_273.oxford_policy_tracker_523` t0 SET t0.mobility = STRUCT<
avg_retail FLOAT64, avg_grocery FLOAT64, avg_parks FLOAT64, avg_transit FLOAT64, avg_workplace FLOAT64, avg_residential FLOAT64
>
(t1.avg_retail, t1.avg_grocery, t1.avg_parks, t1.avg_transit, t1.avg_workplace, t1.avg_residential)
FROM ( SELECT country_region, date, 
      AVG(retail_and_recreation_percent_change_from_baseline) as avg_retail,
      AVG(grocery_and_pharmacy_percent_change_from_baseline)  as avg_grocery,
      AVG(parks_percent_change_from_baseline) as avg_parks,
      AVG(transit_stations_percent_change_from_baseline) as avg_transit,
      AVG( workplaces_percent_change_from_baseline ) as avg_workplace,
      AVG( residential_percent_change_from_baseline)  as avg_residential
      FROM `bigquery-public-data.covid19_google_mobility.mobility_report`
      GROUP BY country_region, date) AS t1
WHERE t0.country_name = t1.country_region
AND t0.date = t1.date;

select distinct country_name from `qwiklabs-gcp-03-f74398bc23c1.covid_273.oxford_policy_tracker_523` t0 where t0.population is null union all select distinct country_name from `qwiklabs-gcp-03-f74398bc23c1.covid_273.oxford_policy_tracker_523` where country_area is null order by country_name asc;

