SELECT * FROM layoff;

/*1.duplicate records handling
  2.null and blNK Vlues
  3.standardize the data
  4.remove unwanted columns
  create the table that is same as the raw data table that is called 
  staging data were we can perform our queries on staging table so there will be no effect to our raw data*/
  
  create table layoff_stage like layoff;
  select * from layoff_stage;
  insert layoff_stage select * from layoff;
    select * from layoff_stage;
    
/*duplicate stage table is created and all raw data is imported into it*/

select *,row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) 
as row_num from 
layoff_stage;  /* date is a keyword in mysql so it is written in back quotes */

with duplicate_cte as (
select *,row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) 
as row_num from 
layoff_stage)
select * from duplicate_cte where row_num > 1;

/*a row with num greater then one means it is duplicate all values are fatched*/

CREATE TABLE `layoff_stage2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


insert into layoff_stage2
select *,row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) 
as row_num from 
layoff_stage;

select * from layoff_stage2 where row_num > 1;
SET SQL_SAFE_UPDATES = 0;     /* to disable the safe updates query is used*/


delete from layoff_stage2 where row_num > 1;

select *  from layoff_stage2 where row_num > 1;

select * from layoff_stage2;

/*-----------------------------------------standardizing the data-----------------------------------------*/

select company,trim(company) from layoff_stage2;

update layoff_stage2 set company=trim(company);  /* while entring data there may be space difference so to avoid it we use trim*/

select distinct industry from layoff_stage2 order by 1;    /* use this type query for every col to check the data*/

select * from layoff_stage2 where industry like 'Crypto%';

update layoff_stage2 set industry='Crypto' where industry like 'Crypto%';

select * from layoff_stage2 where industry like 'Crypto%';

select distinct country from layoff_stage2 order by 1; 

select distinct country,trim(trailing '.' from country) from layoff_stage2 order by 1; 

update layoff_stage2 set country=trim(trailing '.' from country) where country like 'United States%';

select * from layoff_stage2;

/*------------------------------------WORKING ON DATE COLUMN-----------------------------------------------------------*/

select `date` from layoff_stage2;

select `date`,str_to_date(`date`,'%m/%d/%Y') from layoff_stage2;  /* converting date which is text form to date form*/

update layoff_stage2 set `date` = str_to_date(`date`,'%m/%d/%Y');

select * from layoff_stage2;

alter table layoff_stage2 modify column `date` date;  /* converting date which is text datatype to date datatype*/

select * from layoff_stage2;

/*----------------------------------------HANDELING THE NULL VALUES------------------------------------------*/

select * from layoff_stage2 where total_laid_off is null and percentage_laid_off is null;   /* for no use*/

delete from layoff_stage2 where total_laid_off is null and percentage_laid_off is null;

select * from layoff_stage2 where total_laid_off is null and percentage_laid_off is null;



select * from layoff_stage2 where industry is null or industry='';

update layoff_stage2 set industry = null where industry='';

select * from layoff_stage2 where industry is null;

select company,location,industry from layoff_stage2 where company in ('Airbnb','Carvana','Juul') or company="'Bally's Interactive";

update layoff_stage2 set industry='Travel' where company='Airbnb';              /* update the industry with null values with the help of industry with values*/
update layoff_stage2 set industry='Transportation' where company='Carvana';         
update layoff_stage2 set industry='Consumer' where company='Juul';

select company,location,industry from layoff_stage2 where company in ('Airbnb','Carvana','Juul') or company="'Bally's Interactive";

select * from layoff_stage2 where industry is null;

delete from layoff_stage2 where company="Bally's Interactive";

select * from layoff_stage2;


/*--------------------------------------------DELETING THE COLUMNS-----------------------*/

alter table layoff_stage2 drop column row_num;


select * from layoff_stage2;
