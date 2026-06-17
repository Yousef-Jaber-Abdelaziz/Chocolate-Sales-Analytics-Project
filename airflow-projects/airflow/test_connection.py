from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

# The exact connection string from your prompt
pg_uri = 'postgresql://dwh_user:dwh_password@postgres:5432/dwh'

print(f'Attempting to connect to: {pg_uri}')

try:
    engine = create_engine(pg_uri)
    with engine.connect() as connection:
        print(' SUCCESS! The Python connection works perfectly.')
except OperationalError as e:
    print(f' FAILED! Could not connect.')
    print(f'Error details: {e}')
except Exception as e:
    print(f' FAILED with an unexpected error: {e}')
