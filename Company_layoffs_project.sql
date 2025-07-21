SELECT *
FROM layoffs;
-- TASKS 
-- 1.remove duplicates
-- 2.standardize the data
-- 3.Null value
-- 4.remove unnecessary columns

-- -----------------------------------------------------1.remove duplicates-----------------------------------------------------------------

-- create a staging table so real raw data is not disturbed
CREATE TABLE layoffs_staging
LIKE layoffs;
INSERT INTO layoffs_staging
SELECT *
FROM layoffs;


SELECT *
FROM layoffs_staging;


SELECT *, ROW_NUMBER() 
OVER(PARTITION BY company,location, industry, total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions) as serial
FROM layoffs_staging;

WITH duplicate_CTE AS
(
SELECT *, ROW_NUMBER() 
OVER(PARTITION BY company,location, industry, total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions) as serial
FROM layoffs_staging
)
SELECT * 
FROM duplicate_CTE 
WHERE serial > 1;

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
  `serial` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *, ROW_NUMBER() 
OVER(PARTITION BY company,location, industry, total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions) as serial
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2;

DELETE 
FROM layoffs_staging2
WHERE serial>1;

SET SQL_SAFE_updates=0;


-- ------------------------------------------------2.standardize the data----------------------------------------------------------------
-- lineswise see all the columns and make corrections 
SELECT DISTINCT company
FROM layoffs_staging2;
UPDATE layoffs_staging2
SET company= TRIM(company);

SELECT DISTINCT industry -- just like this scan every column and correct if you see any problem 
FROM layoffs_staging2;

UPDATE layoffs_staging2  -- there were rows with names Crypto, CryptoCurrency, Crypto Currency 
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging  -- 'United States' and 'United States.' dono the  
SET country = TRIM(TRAILING '.' FROM country)  -- can be done by SET country = 'United States' also
WHERE country LIKE 'United States%';

UPDATE layoffs_staging2          -- date column was of text datatype 
SET `date`= str_to_date(`date`, '%m/%d/%Y');
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

UPDATE layoffs_staging2    -- some rows had missing value or null in industry column
SET industry = NULL
WHERE industry = '';

SELECT t1.industry,t2.industry
FROM layoffs_staging2 AS t1
INNER JOIN layoffs_staging2 AS t2
ON t1.company=t2.company
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
ON t1.company=t2.company
SET t1.industry=t2.industry
WHERE t1.industry IS NULL 
AND t2.industry is NOT NULL;

SELECT *                          -- removing unnecessary rows 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2     -- dropping the column we added 
DROP COLUMN serial;

SELECT *                           -- cleaned data 
FROM layoffs_staging2;


-- ------------------------------------------------------ Exporatory Data Analysis -----------------------------------------------------
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT company, SUM(total_laid_off) AS TOTAL_LAID_OFF
FROM layoffs_staging2
GROUP BY company
ORDER BY TOTAL_LAID_OFF DESC;

SELECT MIN(`date`) , MAX(`date`)
FROM layoffs_staging2;

SELECT industry, SUM(total_laid_off) AS TOTAL_LAID_OFF
FROM layoffs_staging2
GROUP BY industry
ORDER BY TOTAL_LAID_OFF DESC;

SELECT country, SUM(total_laid_off) AS TOTAL_LAID_OFF
FROM layoffs_staging2
GROUP BY country
ORDER BY TOTAL_LAID_OFF DESC;

SELECT YEAR(`date`), SUM(total_laid_off) AS TOTAL_LAID_OFF
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off) AS TOTAL_LAID_OFF
FROM layoffs_staging2
GROUP BY stage
ORDER BY TOTAL_LAID_OFF DESC;

SELECT SUBSTRING(`date`,1,7) AS `MONTH` , SUM(total_laid_off) 
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

WITH Rolling_Total AS                -- rolling sum of the layoffs 
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH` , SUM(total_laid_off) AS Total_Laid
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`,Total_Laid, SUM(Total_Laid) OVER(ORDER BY `MONTH`) AS Rollong_Total
FROM Rolling_Total;


SELECT company, YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year(Company,`Year`, Total_Laid_off) AS
(
SELECT company, YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS 
(SELECT *, DENSE_RANK() OVER(PARTITION BY `Year` ORDER BY Total_Laid_off DESC) AS Ranking
FROM Company_Year
WHERE Year IS NOT NULL
)
SELECT * 
FROM Company_Year_Rank
WHERE Ranking <6;