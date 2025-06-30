-- database creation
create database project;

-- data loading check
SELECT * FROM project.Netflix_raw limit 10; 

describe project.Netflix_raw;

-- checking for duplicates
select lower(title),type from 
project.Netflix_raw
group by 1,2 having count(*)>1;

-- unique records from netflix data
drop table if exists project.netflix_dedup_data;
create table project.netflix_dedup_data as
select * from (
select *, row_number() over(partition by title,type order by show_id) as rnk
from project.Netflix_raw) a where rnk=1;

-- creating separate dataset for show_id and director
drop table if exists project.show_id_director;
create table project.show_id_director as
WITH RECURSIVE director_split AS (
  SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(director, ',', 1)) AS value,
    SUBSTRING(director, LENGTH(SUBSTRING_INDEX(director, ',', 1)) + 2) AS rest
  FROM project.netflix_dedup_data
  WHERE director IS NOT NULL

  UNION ALL

  SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(rest, ',', 1)),
    SUBSTRING(rest, LENGTH(SUBSTRING_INDEX(rest, ',', 1)) + 2)
  FROM director_split
  WHERE rest IS NOT NULL AND rest != ''
)
SELECT show_id, value AS director
FROM director_split;

-- creating separate dataset for show_id and country
drop table if exists project.show_id_country;
create table project.show_id_country as
WITH RECURSIVE country_split AS (
  SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(country, ',', 1)) AS value,
    SUBSTRING(country, LENGTH(SUBSTRING_INDEX(country, ',', 1)) + 2) AS rest
  FROM project.netflix_dedup_data
  WHERE country IS NOT NULL

  UNION ALL

  SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(rest, ',', 1)),
    SUBSTRING(rest, LENGTH(SUBSTRING_INDEX(rest, ',', 1)) + 2)
  FROM country_split
  WHERE rest IS NOT NULL AND rest != ''
)
SELECT show_id, value AS country
FROM country_split;

-- creating separate dataset for show_id and cast
drop table if exists project.show_id_cast;
create table project.show_id_cast as
WITH RECURSIVE cast_split AS (
  SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(cast, ',', 1)) AS value,
    SUBSTRING(cast, LENGTH(SUBSTRING_INDEX(cast, ',', 1)) + 2) AS rest
  FROM project.netflix_dedup_data
  WHERE cast IS NOT NULL

  UNION ALL

  SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(rest, ',', 1)),
    SUBSTRING(rest, LENGTH(SUBSTRING_INDEX(rest, ',', 1)) + 2)
  FROM cast_split
  WHERE rest IS NOT NULL AND rest != ''
)
SELECT show_id, value AS cast
FROM cast_split;

-- creating separate dataset for show_id and listed_in
drop table if exists project.show_id_listed_in;
create table project.show_id_listed_in as
WITH RECURSIVE listed_in_split AS (
  SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(listed_in, ',', 1)) AS value,
    SUBSTRING(listed_in, LENGTH(SUBSTRING_INDEX(listed_in, ',', 1)) + 2) AS rest
  FROM project.netflix_dedup_data
  WHERE listed_in IS NOT NULL

  UNION ALL

  SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(rest, ',', 1)),
    SUBSTRING(rest, LENGTH(SUBSTRING_INDEX(rest, ',', 1)) + 2)
  FROM listed_in_split
  WHERE rest IS NOT NULL AND rest != ''
)
SELECT show_id, value AS listed_in
FROM listed_in_split;

-- handling the missing values from country column
insert into project.show_id_country
select a.show_id, b.country
from project.netflix_dedup_data a
inner join
(select distinct a.director, b.country
from project.show_id_director a
inner join project.show_id_country b
on a.show_id=b.show_id) b
on a.director=b.director
where a.country is null;

-- checking the null values of duration
select * from project.netflix_dedup_data where duration is null;

-- final netflix data with all transformations by taking needed columns

drop table if exists project.final_netfix_data;
create table project.final_netfix_data as
select show_id, type, title, cast(str_to_date(date_added,'%M %d, %Y') as date) as date_added, release_year, rating,
case when duration is null then rating else duration end as duration,
description from project.netflix_dedup_data;

select * from project.final_netfix_data limit 10;

-- data analysis

-- 1. for each director count the no of movies and tv shows created by them in separate columns for directors who have created tv shows and movies both

select b.director,
count(distinct case when a.type='Movie' then a.show_id end) as no_of_movies,
count(distinct case when a.type='TV Show' then a.show_id end) as no_of_tv_show
from project.final_netfix_data a
inner join
project.show_id_director b
on a.show_id=b.show_id
group by 1 having count(distinct a.type)>1;

-- 2. which country has highest number of comedy movies

select  b.country, count(distinct a.show_id) as comedy_movies
from project.show_id_listed_in a
inner join 
project.show_id_country b
on a.show_id=b.show_id
inner join
project.final_netfix_data c
on a.show_id=c.show_id
where a.listed_in='Comedies' and c.type='Movie'
group by 1 order by 2 desc limit 1;

-- 3. for each year (as per date added to netflix), which director has maximum number of movies released

select director, release_year, no_of_movies from
(select *,
row_number() over(partition by release_year order by no_of_movies desc, director) as rnk
from
(select b.director,year(a.date_added) as release_year, count(distinct a.show_id) as no_of_movies
from project.final_netfix_data a
inner join 
project.show_id_director b
on a.show_id=b.show_id
where a.type='Movie'
group by 1,2) a ) a where rnk=1;

-- 4. what is average duration of movies in each genre

select b.listed_in, round(avg(cast(replace(duration,' min','') as unsigned)),0) avg_duration
from project.final_netfix_data a
inner join 
project.show_id_listed_in b
on a.show_id=b.show_id
where a.type='Movie'
group by 1;

-- 5. find the list of directors who have created horror and comedy movies both.
-- display director names along with number of comedy and horror movies directed by them
select c.director,
count(distinct case when listed_in='Comedies' then a.show_id end) as no_of_comedy_movies,
count(distinct case when listed_in='Horror Movies' then a.show_id end) as no_of_horror_movies
from project.final_netfix_data a
inner join 
project.show_id_listed_in b
on a.show_id=b.show_id
inner join 
project.show_id_director c
on a.show_id=c.show_id
where a.type='Movie' and b.listed_in in ('Comedies','Horror Movies')
group by 1 having count(distinct b.listed_in)>1
;