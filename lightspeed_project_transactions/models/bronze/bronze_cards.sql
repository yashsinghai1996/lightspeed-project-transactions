select *
from {{ source('source', 'cards_data') }}