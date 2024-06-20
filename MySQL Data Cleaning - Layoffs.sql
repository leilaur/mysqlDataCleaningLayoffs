-- CREATE SCHEMA AND IMPORT TABLE

SELECT *
FROM layoffs;

-- CREATE A STAGING TABLE TO WORK ON (TO PRESERVE THE RAW DATA FOR REFERENCE IF NEEDED)

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

SELECT * 
FROM layoffs_staging;

-- PART 1: REMOVING DUPLICATES

WITH duplicate_CTE AS 
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_CTE
WHERE row_num > 1;

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- PART 2: STANDARDIZING DATA

-- COMPANY

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- INDUSTRY

SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY industry;  -- Presence of Crypto, Crypto Currency and CryptoCurrency

SELECT * 
FROM layoffs_staging2
WHERE industry LIKE 'crypto%'; -- Update Crypto Currency and CryptoCurrency to Crypto since 'Crypto' is the majority

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%';

-- LOCATION

SELECT DISTINCT(location)
FROM layoffs_staging2
ORDER BY location; -- No issues found

-- COUNTRY

SELECT DISTINCT(country), TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- DATE - change from text to date format

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;

-- PART 3: LOOK AT NULL AND BLANK VALUES; POPULATE IF POSSIBLE

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company IN ('Airbnb', "Bally's Interactive", 'Carvana', 'Juul');

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
	AND t2.industry IS NOT NULL;
    
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;
    
SELECT *
FROM layoffs_staging2
WHERE company LIKE "Bally's Interactive"; -- no reference industry found

-- total_laid_off and percentage_laid_off cannot be populated due to lack of data

-- PART 4: REMOVE UNNECESSARY COLUMNS/ROWS

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;






