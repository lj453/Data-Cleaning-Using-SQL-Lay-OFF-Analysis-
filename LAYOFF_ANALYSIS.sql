-- Layoff Analysis 

-- Data Cleaning 

-- 1.Seelcting the Table 
SELECT * 
FROM layoffs;

-- 2.Removing Duplicates
-- 3.Standardize the Data
-- 4.Null Values or Blank Values 
-- 5. Remove Any Cloumns or Rows if Not Necessary 


CREATE TABLE layoffs_staging 
LIKE layoffs;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging
SELECT * 
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER (PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as Row_Num
FROM layoffs_staging;

With Duplicate_CTE as
(
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as Row_Num
	FROM layoffs_staging
)
SELECT * 
FROM Duplicate_CTE
where Row_Num =2;

-- In My SQL we can't use delete Option inside a CTE .So instead we copy the contents into another table with added column Row_num and operate on that Table.


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `Row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2;

INSERT INTO  layoffs_staging2
SELECT *,
ROW_NUMBER() OVER 
(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions
) as Row_Num
FROM layoffs_staging;

SELECT * 
FROM layoffs_staging2
WHERE Row_num>1;

SELECT * 
FROM layoffs_staging2
WHERE company='Cazoo';

DELETE
FROM layoffs_staging2
WHERE Row_num > 1;

SELECT * 
FROM layoffs_staging2
WHERE Row_num>1;

-- Standarize the Data 

SELECT * 
FROM layoffs_staging2;

SELECT DISTINCT company,(TRIM(company))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = (TRIM(company));

SELECT DISTINCT industry 
FROM layoffs_staging2
ORDER BY 1;

SELECT * 
FROM layoffs_staging2
where industry like 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry like 'Crypto%';

SELECT DISTINCT country 
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country , TRIM(TRAILING '.' FROM  country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM  country);

-- Convert Date into MySQL Standard Date 

SELECT `date`,
str_to_date(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`,'%m/%d/%Y');

-- Change the Data type of date to date format 

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- REMOVING and Replacing NULL VALUES

 
SELECT * 
FROM layoffs_staging2;

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off is NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off=''
OR percentage_laid_off='';

SELECT * 
FROM layoffs_staging2
WHERE industry=''
OR industry is NULL;

SELECT * 
FROM layoffs_staging2
WHERE company IN ('Airbnb','Carvana','Juul','Bally''s Interactive');

SELECT t1.company, t1.industry,t2.company,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company=t2.company AND
    t1.location=t2.location
    WHERE t1.industry='' OR t1.industry IS NULL 
    AND t2.industry is NOT NULL;
    
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry='';
    
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company=t2.company AND
    t1.location=t2.location
SET t1.industry=t2.industry
WHERE (t1.industry='' OR t1.industry IS NULL )
    AND t2.industry is NOT NULL;
    
SELECT t1.company, t1.industry,t2.company,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company=t2.company AND
    t1.location=t2.location;

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off is NULL 
	AND percentage_laid_off IS NULL;
    
DELETE 
FROM layoffs_staging2
WHERE total_laid_off is NULL 
	AND percentage_laid_off IS NULL;
    
ALTER TABLE layoffs_staging2
DROP COLUMN Row_num;


SELECT * 
FROM layoffs_staging2;









