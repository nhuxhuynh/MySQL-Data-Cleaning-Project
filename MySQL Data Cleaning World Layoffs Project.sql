-- Data Cleaning Project on World Layoffs

SELECT *
FROM layoffs;

-- Step 1: Remove Duplicates (Check for duplicates and remove them)
-- Step 2: Standardize the Data (If there are issues with Data then standardize it)
-- Step 3: Null values or Blank Values (See if we need to/can populate the values)
-- Step 4: Remove Any Columns (If needed to, we will remove any unnecessary column(s))


-- This process is to create a different table so that when we change the database a lot, we can still have the raw data availabe

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;


-- Steps 1: Remove Duplicates

# Check for any duplicates
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

# Can't exactly delete duplicate from previous table so Creating another table in order to actually be able to DELETE duplicates
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
FROM layoffs_staging2
WHERE row_num > 1;

# Insert data from layoffs_staging table into layoffs_staging2 table
INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num
FROM layoffs_staging;

-- Remove duplicates from layoffs_stagings
DELETE FROM layoffs_staging2
WHERE row_num > 1;

# check a company, for example: 'Casper' to make sure code works
SELECT *
FROM layoffs_staging2
WHERE company = 'Casper';

-- Step 2: Standardizing Data

# Fix white space
SELECT company, (TRIM(company))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

# Update All Crypo% to be just Crypto
SELECT DISTINCT industry
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

# Fix the . at the end of the United States, so all is just United States
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

# Time series exploratory data analysis/visualizations, then we need to change/format date from (string) text to date
SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y'); # capital %Y for output of 4 number long year

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- Step 3: Null and Blank Values

# Check to see the Null and Blanks, for total laid off and percentage laid off we want to leave it for now
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# Null and Blank Values in industry is populated with a value that matches with another same existing Company's industry
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT * 
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 
	ON t1.company = t2.company 
    AND t2.industry IS NOT NULL AND t2.industry != ''
SET t1.industry = t2.industry
WHERE t1.industry IS NULL OR t1.industry = '';


-- Step 4: Remove Any Columns

# removing percentage laid off and total laid off column because of NULL 
SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# remove the unnecessary row_num and now we can execute Select * From layoffs_staging2 for our final result
SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
