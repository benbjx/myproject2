INSERT INTO `{{ params.environnement }}.DWH.dim_employee_histo`
select id, userprincipalname
from `{{ params.environnement }}.DSG.dim_employe`
where date_of_day {{ params.pattern_date }}