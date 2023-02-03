--Append this SQL if downsampling clients is desired.

)
SELECT
  * EXCEPT(clients_per_segment)
FROM
  clients_weekly
WHERE
  RAND() < @sample / clients_per_segment
