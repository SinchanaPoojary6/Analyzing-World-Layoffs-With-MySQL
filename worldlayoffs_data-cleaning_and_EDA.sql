-- Data Cleaning --
SELECT *
FROM layoffs;


-- 1. Remove Suplicates--
-- 2. Standardize Data--
-- 3. Null values or blanks--
-- 4. Remove unused columns--

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

WITH duplicate_cte AS
(
SELECT *,
row_number() over( partition by 
company,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num>1;

WITH duplicate_cte AS
(
SELECT *,
row_number() over( partition by 
company,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE 
FROM duplicate_cte
WHERE row_num>1;

CREATE TABLE layoffs_staging1
LIKE layoffs;
INSERT layoffs_staging1
SELECT *
FROM layoffs;
SELECT *
FROM layoffs_staging1;
 ALTER TABLE layoffs_staging1 ADD COLUMN row_num INT;
TRUNCATE TABLE layoffs_staging1;

INSERT layoffs_staging1
SELECT *,
row_number() over( partition by 
company,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging1
WHERE row_num>1;

DELETE
FROM layoffs_staging1
WHERE row_num>1;

-- Standardizing Data--

SELECT DISTINCT(TRIM(company))
FROM layoffs_staging1;

SELECT company,(TRIM(company))
FROM layoffs_staging1;

UPDATE layoffs_staging1
SET company= TRIM(company);

SELECT *
FROM layoffs_staging1
WHERE industry LIKE '%crypto%';

UPDATE layoffs_staging1
SET industry='Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layoffs_staging1;

SELECT DISTINCT country
FROM layoffs_staging1
WHERE country LIKE '%United States%'
ORDER BY 1;

UPDATE layoffs_staging1
SET industry='United States'
WHERE industry LIKE 'United States%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging1
ORDER BY 1;

UPDATE layoffs_staging1
SET country=TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT DISTINCT country
FROM layoffs_staging1
ORDER BY 1;

SELECT `date`,
str_to_date(`date`,'%m/%d/%Y')
FROM layoffs_staging1;

UPDATE layoffs_staging1
SET `date`=str_to_date(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_staging1
MODIFY COLUMN `date` DATE;

DELETE
FROM layoffs_staging1
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

UPDATE layoffs_staging1
SET industry=NULL
WHERE industry='';

SELECT *
FROM layoffs_staging1
WHERE industry IS NULL OR industry='';

SELECT *
FROM layoffs_staging1
WHERE company='Airbnb';

SELECT *
FROM layoffs_staging1 st1
JOIN  layoffs_staging1 st2
ON st1.company=st2.company
AND st1.location=st2.location
WHERE (st1.industry IS NULL OR st1.industry='') AND st2.industry IS NOT NULL;

UPDATE layoffs_staging1 st1
JOIN  layoffs_staging1 st2
ON st1.company=st2.company 
SET st1.industry=st2.industry
WHERE st1.industry IS NULL AND st2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging1
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs_staging1;

ALTER TABLE layoffs_staging1
DROP COLUMN row_num;

-- Exploratory Data Analysis--

SELECT MAX(total_laid_off),MAX(percentage_laid_off)
FROM layoffs_staging1;

SELECT *
FROM layoffs_staging1
WHERE percentage_laid_off=1
ORDER BY funds_raised_millions DESC;

SELECT company,SUM(total_laid_off)
FROM layoffs_staging1
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`),MAX(`date`)
FROM layoffs_staging1;

SELECT industry,SUM(total_laid_off)
FROM layoffs_staging1
GROUP BY industry
ORDER BY 2 DESC;

SELECT country,SUM(total_laid_off)
FROM layoffs_staging1
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging1
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT stage,SUM(total_laid_off)
FROM layoffs_staging1
GROUP BY stage
ORDER BY 2 DESC;

SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off)
FROM layoffs_staging1
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC;

WITH Rolling_total AS(
SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off) As total_laidoff
FROM layoffs_staging1
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC)
SELECT `Month`, total_laidoff, SUM(total_laidoff) OVER (ORDER BY `Month`) AS rolling_total
FROM Rolling_total;

SELECT company,YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging1
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year (company,years,total_laid_off) AS (
SELECT company,YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging1
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS (
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL)
SELECT *
FROM Company_Year_Rank
WHERE Ranking<=5;



