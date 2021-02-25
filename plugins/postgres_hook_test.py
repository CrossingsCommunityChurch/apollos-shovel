from airflow.hooks.postgres_hook import PostgresHook


def fix_casing(col):
    return "\"{}\"".format(col)

print(PostgresHook._generate_insert_sql(
    'people',
    ("DateTime(2021, 2, 25, 0, 0, 0, tzinfo=Timezone('+00:00'))", "DateTime(2021, 2, 25, 0, 0, 0, tzinfo=Timezone('+00:00'))", 616885, 'rock', 'Person', 'Kara', 'Wacenske', 'FEMALE', '2002-08-03T00:00:00', 'wacenskekara@icloud.com', '06ee560d-ee17-478c-b524-370e4bffd7f5'),
    list(map(fix_casing, ("createdAt", "updatedAt", "originId", "originType", "apollosType", "firstName", "lastName", "gender", "birthDate", "campusId", "email"))),
    True,
    replace_index = ('"originId"', '"originType"')
))

