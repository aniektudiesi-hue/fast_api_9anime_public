# Supabase database on Render

The backend persists users, passwords, watch history, watchlist, visits, and chat in Postgres when `DATABASE_URL` is set.

## Supabase project

- Project: `hackathon`
- Project ref: `atkmmdigdmkkrjheonuk`
- Region: `us-east-1`

## Render environment variable

Set this in the Render service environment:

```text
DATABASE_URL=<your Supabase Postgres connection string>
PGSSLMODE=require
```

Use the Supabase dashboard:

1. Open Supabase Dashboard.
2. Open project `hackathon`.
3. Click `Connect`.
4. Copy the `Session pooler` URI if available. It supports IPv4 and works well for a long-running Render web service.
5. Replace the password placeholder with the real database password.
6. Save it as Render env var `DATABASE_URL`.

Example shape only, do not commit the real password:

```text
postgres://postgres.atkmmdigdmkkrjheonuk:[YOUR-PASSWORD]@aws-0-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require
```

The app also disables psycopg prepared statements, so a transaction-pooler URI can work too if that is the only URI available.

After redeploy, check:

```text
https://anime-search-api-burw.onrender.com/health
```

Expected result:

```json
{
  "db_backend": "postgres",
  "persistent": true
}
```

If it says `"db_backend": "sqlite"` or `"persistent": false`, Render does not have `DATABASE_URL` set on the live service yet.
