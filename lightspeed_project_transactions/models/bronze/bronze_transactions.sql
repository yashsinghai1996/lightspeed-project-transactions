select *
from {{ source('source', 'transactions_data') }}