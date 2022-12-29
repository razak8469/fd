select
    *,
    case 
        when extract(hour from TX_DATETIME) <= 6 then 1 
        else 0 
    end as TX_DURING_NIGHT,
    case 
        when extract(dayofweek from TX_DATETIME) in (1, 7) then 1 
        else 0 
    end as TX_DURING_WEEKEND,
    avg(TX_AMOUNT) over(partition by CUSTOMER_ID order by unix_seconds(TX_DATETIME) range between 86400 preceding and current row ) as CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW,
    count(*) over(partition by CUSTOMER_ID order by unix_seconds(TX_DATETIME) range between 86400 preceding and current row) as CUSTOMER_ID_NB_TX_1DAY_WINDOW,
    
    avg(TX_AMOUNT) over(partition by CUSTOMER_ID order by unix_seconds(TX_DATETIME) range between 604800 preceding and current row) as CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW,
    count(*) over(partition by CUSTOMER_ID order by unix_seconds(TX_DATETIME) range between 604800 preceding and current row) as CUSTOMER_ID_NB_TX_7DAY_WINDOW,
   
    avg(TX_AMOUNT) over(partition by CUSTOMER_ID order by unix_seconds(TX_DATETIME) range between 2592000 preceding and current row) as CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW,
    count(*) over(partition by CUSTOMER_ID order by unix_seconds(TX_DATETIME) range between 2592000 preceding and current row) as CUSTOMER_ID_NB_TX_30DAY_WINDOW,

    avg(TX_FRAUD) over(partition by TERMINAL_ID order by unix_seconds(TX_DATETIME) range between 86400 preceding and current row ) as TERMINAL_ID_RISK_1DAY_WINDOW,
    count(*) over(partition by TERMINAL_ID order by unix_seconds(TX_DATETIME) range between 86400 preceding and current row) as TERMINAL_ID_NB_TX_1DAY_WINDOW,
    
    avg(TX_FRAUD) over(partition by TERMINAL_ID order by unix_seconds(TX_DATETIME) range between 604800 preceding and current row) as TERMINAL_ID_RISK_7DAY_WINDOW,
    count(*) over(partition by TERMINAL_ID order by unix_seconds(TX_DATETIME) range between 604800 preceding and current row) as TERMINAL_ID_NB_TX_7DAY_WINDOW,
   
    avg(TX_FRAUD) over(partition by TERMINAL_ID order by unix_seconds(TX_DATETIME) range between 2592000 preceding and current row) as TERMINAL_ID_RISK_30DAY_WINDOW,
    count(*) over(partition by TERMINAL_ID order by unix_seconds(TX_DATETIME) range between 2592000 preceding and current row) as TERMINAL_ID_NB_TX_30DAY_WINDOW,
from 
    {{DATASET_IN}}.{{TABLE_IN}}
