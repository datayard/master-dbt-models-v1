# Vidyard Unified Record

This project defines the rules that merge Vidyard business data sources into a single unified record of organizations and people. It uses the [dbt](https://github.com/fishtown-analytics/dbt) open source project to define a structure of SQL models for cleaning, aggregating, and merging multiple data sources.

## Data Sources

`modes/schema.yml` defines the data source inputs including Salesforce, Clearbit (via Salesforce), and Vidyard transactional data


## Output

The resulting merged tables are:

- `organizations.sql` - A list of all organizations, identified by domain name
- `people.sql` - A list of all people, identified by email address
- `people_to_organizations.sql` - The membership of people to organizations

## Running

This git repo is automatically executed by Census on a regular basis. Any errors in the models will be captured by the Census model builder.

## Sideloading domain blacklists

```
CREATE TABLE IF NOT EXISTS census.world_universities_domains (
  domain VARCHAR
);

COPY census.world_universities_domains
FROM 's3://public-lists/world_universities_domains.csv'
ACCESS_KEY_ID '[INSERT HERE]'
SECRET_ACCESS_KEY '[INSERT HERE]';

SELECT * FROM census.world_universities_domains
WHERE domain IN (SELECT domain FROM census.domains);

```