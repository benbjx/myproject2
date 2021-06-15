INSERT INTO `{{ params.environnement }}.DWH.dim_employee_histo`
select *
from `{{ params.environnement }}.DSG.dim_employee`
where date_of_day {{ params.pattern_date }}