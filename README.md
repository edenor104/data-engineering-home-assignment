# My Solution for  Data Engineering Assignment (PySpark)

## Requirements:

This is an assignment for a Data Engineer role. You are requested to:

-   Read and understand the requirements. You may contact the interviewer for further clarification
-   Write code that answers the objectives
-   Deploy the code to the provided AWS account

You are a Data Engineer in a financial institute. Your task is to calculate and answer the business questions (objectives) provided by the analysts team. You’re provided here with a small dataset, but in a real-world scenario you'll have a huge dataset, so the code needs to be deployed and run on a cloud environenment.

## Coding instructions:

-   The file `stocks_data.csv` contains daily closing price of a few stocks on the NYSE/NASDAQ
-   Load the file as a DataFrame, Dataset, or RDD and complete the assignment objectives
-   The result of each question should be saved as a separate file in an S3 Bucket
  
## Personal Code Additions:
-   There are two versions of the code: one for local runs (which still saves to S3), and another for the Glue job. The Glue job is preferable for handling larger-scale data in the future.
-   In general, I tried to add more configurable parameters in case different analyses are needed in the future (i.e., a different number of days for Objective 4, or changing how many results to display in the top list).


## My Assumptions:
-   There may be duplicates in the data.
-   In real life, there should not be more than one value per stock per day (this should be the natural PK)
-   It's better to remove missing values than to fill them in—filling a missing day using an adjacent outlier could lead to inaccuracies.
-   Annualized standard deviation of daily returns is calculated by std(daily_std_per_stock)*sqrt(252 days)
-   Since the last question asked for dates, it's OK to have the top 3 from the same stock (TESLA)
-   It's fine to display the top 3 for Objectives 2 and 3 — seeing a slightly broader picture doesn’t hurt in this case.
-   Same goes for displaying the percentage return in Objective 4 — it serves as a good sanity check to ensure everything is working correctly.

 
## Assumptions:

-   Use only the closing price to determine returns
-   If a price is missing on a given date, you can compute returns from the closest available date
-   Return can be trivially computed as the % difference of two prices

## Objectives:

1. Compute the average daily return of all stocks for every date

    | date       | average_return                    |
    | ---------- | --------------------------------- |
    | yyyy-MM-dd | return of all stocks on that date |

2. Which stock was traded with the highest worth - as measured by **closing price \* volume** - on average?

    | ticker | value |
    | ------ | ----- |
    |        |       |

3. Which stock was the most volatile as measured by the annualized standard deviation of daily returns?

    | ticker | standard_deviation |
    | ------ | ------------------ |
    |        |                    |

4. What were the top three 30-day return dates as measured by % increase in closing price compared to the closing price 30 days prior? present the top three ticker and date combinations.

    | ticker | date |
    | ------ | ---- |
    |        |      |

## AWS Deployment

-   At Vi, we manage and provision cloud infrastructure through definition files ([IaC](https://en.wikipedia.org/wiki/Infrastructure_as_code)). Please use IaC (such as CloudFormation) to deploy the code you created to perform the below tasks.
-   Infrastructure tasks:
    -   Create a glue job
    -   Create a Glue Catalog Database
    -   Create a Glue Catalog Table for each result file
    -   Create crawler/s
-   **Expected result: Questions (objectives) results should be queryable from Athena**
-   If you encounter issues with reading/writing from/to S3 buckets, it is recommended to add your name as a prefix to the bucket’s name. For example name the bucket: “data-engineer-assignment-my-name”
-   Use the AWS credentials provided in the email to deploy the code. **DO NOT COMMIT THEM IN THE CODE.**
-   Deploy your resources in the **Europe (Frankfurt) eu-central-1**
-   Create a local `.env` file with the following environment variables
    -   AWS_ACCESS_KEY_ID
    -   AWS_SECRET_ACCESS_KEY
    -   STACK_NAME
-   Use the `create-update-stack.sh` in the repo to to deploy your stack file. A demo stack file is provided in the repo

## AWS Deployment Details:
-    bucket: data-engineer-assignment-eden
-    glue db: stock-analysis-db
-    job name: stock-analysis-job




## Evaluation

-   Objectives completion
-   Code quality & efficiency
-   AWS deployment
