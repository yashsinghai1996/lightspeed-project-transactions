select *
from {{ source('source', 'users_data') }}