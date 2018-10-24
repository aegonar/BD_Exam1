CREATE TABLE students (
    region_escuela String,
    distrito_escuela String,
    ident_escuela int,
    nombre_escuela String,
    nivel_escuela String,
    sexo_estudiante char(1),
    ident_estudiante int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- LOAD DATA INPATH '/user/maria_dev/studentsPR' INTO table students;

CREATE TABLE schools (
    region_escuela String,
    distrito_escuela String,
    ciudad_escuela String,
    ident_escuela int,
    nombre_escuela String,
    nivel_escuela String,
    ident_cb int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- LOAD DATA INPATH '/user/maria_dev/escuelasPR' INTO table schools;

-- 1)
select schools.region_escuela,
    schools.ciudad_escuela,
    count(*) 
from students, 
    schools 
where schools.ident_escuela=students.ident_escuela and
    schools.distrito_escuela = students.distrito_escuela and 
    schools.region_escuela = students.region_escuela 
group by schools.region_escuela, 
    schools.ciudad_escuela;

-- 2)
select schools.ciudad_escuela, 
    schools.nivel_escuela,
    count(*) 
from schools
group by schools.ciudad_escuela, 
    schools.nivel_escuela;

-- 3)
select count(*) 
from schools, students 
where schools.ident_escuela = students.ident_escuela and 
    schools.distrito_escuela = students.distrito_escuela and 
    schools.region_escuela = students.region_escuela and 
        schools.nivel_escuela ='Superior' and 
        students.sexo_estudiante = 'F' and 
        schools.ciudad_escuela = 'Ponce';

-- 4)
select schools.region_escuela, 
    schools.distrito_escuela, 
    schools.ciudad_escuela, 
    count(*) 
from schools, students 
where schools.ident_escuela = students.ident_escuela and 
    schools.distrito_escuela = students.distrito_escuela and 
    schools.region_escuela = students.region_escuela and 
        students.sexo_estudiante = 'M' 
group by schools.region_escuela, 
    schools.distrito_escuela, 
    schools.ciudad_escuela;
