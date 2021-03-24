# Vidyard DBT Master Records

This project defines the rules that merge Vidyard business data sources into a single unified record of organizations and people. It uses the [dbt](https://github.com/fishtown-analytics/dbt) open source project to define a structure of SQL models for cleaning, aggregating, and merging multiple data sources.

## Data Sources

`modes/schema.yml` defines the data source inputs from ancillary systems including Salesforce, Marketo, Intercom (via Salesforce), and Vidyard transactional data

## Output

The resulting merged tables are divided into two groups:

###staging models:
- this is the normalized and cleaned raw data, this tables have been passed trough a filter to aling nomenclature and field formatting to them.
- Sub-divided into systems and categories according to the object they represent, they are governed by the set of rules mentioned bellow.

###mart models:
- this is the actionable models that fit our data delivery reports generated for dashboards, mart tables are created with the set of rules mentioned bellow.
- Sub-divided into business units, each business unit holds its own generated data and is constrained by the rules mentioned bellow.

## Modeling rules: staging

###Rule 1: (Location) 
- 1.a All tables generated inside the staging tables need to be hosted inside the folder of the container system i.e: Salesforce account folders will be hosted inside of their origin sytem folder, if said folder doesn't exist it will be created.

###Rule 2: (Naming Conventions)
- 2.a Tables generated inside the folders of the containing systems must carry the nomenclature 'stg_[system]_[object].sql' i.e stg_salesforce_account.sql.
- 2.b The table names will be singular and not plural i.e: 'account' instead of 'accounts'.

###Rule 3:
- 3.a - 

## Modeling rules: marts
