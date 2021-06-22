# Apollos Shovel 

## Abstract

A tool for "shoveling" your church's RMS/CMS data into a Postgres Server you control. Built on Apache Airflow, and powered by Astronomer.io. 

## Development

We use the Astronomer CLI for local development. [Follow their instructions](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart#overview) to install the CLI. Once you have CLI installed, run `astro dev start`. 

## Adding another Client

Adding another client is accomplished by creating the appropriate DAG files in the `dags` folder and adding the appropriate `Airflow Config` values. The steps are as follows. 

1. Within the Airflow UI, add the following Variables
  - `${client_name}_rock_api` - url to the client's Rock instance.
  - `${client_name}_rock_token` - API key for the client's rock instance
2.  Also within the Airflow UI, adding the following Connection
  - `${client_name}_apollos_postgres`
  - Make sure you include the following extra setting if using a Postgres DB 
  ```
  {
    "sslmode": "require"
  }
  ```
3. Create the appropriate DAGs.
  - Copy `rock-people-example-dag.py` and replace the `client: None` variables with `client: ${client_name}`
  - Copy `backfill-rock-people-example-dag.py` and replace the `client: None` variables with `client: ${client_name}`  
  - If you have already deployed Apollos to production without the shovel, Copy `backfill-apollos-user-from-rock-personal-devices-example-dag.py` and replace the `client: None` variables with `client: ${client_name}`
4. Update the `start_date` in all your newly created DAGS to "the correct date"
  - The correct date is whenever you want the DAG to start running :) 
  - It's likely the current date, or even yesterday.
5. You're done! Run the app locally to test your changes and then `astro deploy` to deploy.
