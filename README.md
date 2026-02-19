# Steps

1. Set-up virtual environment:
   1. Create: `py -m venv .venv`
   2. Activate: `.venv\Scripts\activate`
2. Installed dbt modules: `pip install dbt-core dbt-databricks`
3. Installed elementary modules: `pip install elementary-data[databricks]`
4. Set environmental variables using App-Dbricks-Framework-dbt-DEV-001:
   1. Client ID: `set CLIENT_ID=75627542-35f7-47bb-8766-938342842f48`
   2. Client Secret: `set CLIENT_SECRET=<client-secret>` ([App-Dbricks-Framework-dbt-DEV-001-secret-value](https://portal.azure.com/#view/Microsoft_Azure_KeyVault/ListObjectVersionsRBACBlade/~/overview/objectType/secrets/objectId/https%3A%2F%2Fkvdbkdataplatformdev001.vault.azure.net%2Fsecrets%2FApp-Dbricks-Framework-dbt-DEV-001-secret-value/vaultResourceUri/%2Fsubscriptions%2F05275b7e-a0ab-44c2-946b-a2c7bab6e090%2FresourceGroups%2Frg-dataplatform-devqa-001%2Fproviders%2FMicrosoft.KeyVault%2Fvaults%2Fkvdbkdataplatformdev001/vaultId/%2Fsubscriptions%2F05275b7e-a0ab-44c2-946b-a2c7bab6e090%2FresourceGroups%2Frg-dataplatform-devqa-001%2Fproviders%2FMicrosoft.KeyVault%2Fvaults%2Fkvdbkdataplatformdev001/lifecycleState~/null)) > SERVE PARA APP AUTHENTICATION
5. Make sure the elementary dependency is installed in the project: `dbt deps`
6. Dry run of elementary - creates empty tables to be updated later (30 tables): `dbt run --select elementary`
7. Run DBT:
   1. Seed: `dbt seed`
   2. Run models: `dbt run`
8. Test: `dbt test`

# Important

1. **Python Version:** There is a Bug with Elementary and DBT Databricks Adapter which has been fixed in newer Elementary versions.
   1. **Python 3.12.x**: Newer elementary versions and dbt databricks adapeters have the fix, so everything is okay (Databricks 16.4LTS uses version 3.12.3 so everything is okay)
   2. **Python 3.13.x**: Newer elementary versions only support Python 3.12.x (12/12/2025) so downgrade DBT and Elementary versions so there is no conflict.
2. **Name conflict:** DBT Project folder cannot be called "elementary" or it will fail.