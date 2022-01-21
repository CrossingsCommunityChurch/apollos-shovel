# Apollos Shovel

## Abstract

A tool for "shoveling" your church's RMS/CMS data into a Postgres Server you control. Built on Apache Airflow, and powered by Astronomer.io.

## Development

We use the Astronomer CLI for local development. [Follow their instructions](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart#overview) to install the CLI. Once you have CLI installed, run `astro dev start`.

### Troubleshooting Astronomer

If you experience the below error when running `astro dev start`

```
buildkit not supported by daemon
Error: command 'docker build -t apollos-shovel-2_065094/airflow:latest failed: failed to execute cmd: exit status 1
```

Add a new environment variable, DOCKER_BUILDKIT=0, and retry.

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

## Connecting to Postgres Locally

We like to use [Postico](https://eggerapps.at/postico/) to inspect the Postgress DB. Use the following connection details to connect to the Postgress database running locally:

Host: localhost
Port: 5435
User: postgres
Password: postgres
Database: postgres

## Adding Items to the Encrypted Env File

We've got an encrypted environment variable file, `.env.shared.enc` that contains information about airflow variables and connections. If you want to add a variable or connection to this env file (instead of having to manually do so in the airflow UI), do the following:

1. To decrypt the file, run `npx @apollosproject/apollos-cli secrets -d [apollos-shovel-secret]`. The encryption secret is kept in 1Password.
2. Add the new information to the env file. New airflow variables should start with `AIRFLOW_VAR` and a new airflow connection should start with `AIRFLOW_CONN`.
3. Encrypt the file with your changes using `npx @apollosproject/apollos-cli secrets -e [apollos-shovel-secret]`
4. Commit the changes to `.env.shared.enc`
