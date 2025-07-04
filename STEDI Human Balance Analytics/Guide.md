## Project Overview

* **Project Overview**

  * Use **Spark** and **AWS Glue** to process, categorize, and curate multi-source data.
  * Write additional AWS Glue jobs to generate curated step trainer data for machine learning.

* **Project Introduction: STEDI Human Balance Analytics**

  * Role: **Data Engineer** for STEDI team.
  * Goal: Build **data lakehouse** for sensor data used to train ML model.

* **STEDI Step Trainer Details**

  * Hardware device:

    * Trains users in STEDI balance exercise.
    * Contains **motion sensors** to detect object distance.
  * Mobile App:

    * Collects **customer data**.
    * Uses **accelerometer** to detect motion in X, Y, Z directions.
  * Use-case: Detect steps in real-time using ML.
  * Data Privacy:

    * Use only data from customers who **opted in** to share their data.

* **Project Objective**

  * Extract and curate data from:

    * **Step Trainer sensors**
    * **Mobile app accelerometer**
  * Build **data lakehouse** on AWS.
  * Enable Data Scientists to train ML models using curated data.

* **Key Skills Used**

  * **Spark, AWS Glue**
  * Data extraction, transformation, curation
  * Data pipeline development
  * Privacy-aware data handling

* **Evaluation**

  * Work evaluated against a **rubric**.
  * Review rubric requirements throughout project for compliance.


## Environments

* **Project Environment: Cloud-Based Data Lakehouse**

  * **Data Sources:** STEDI Step Trainer & mobile app
  * **Tools/Services Used:**

    * **Python & Spark**
    * **AWS Glue**
    * **AWS Athena**
    * **AWS S3**

* **AWS Environment:**

  * Temporary AWS account provided
  * **\$25 budget limit**
  * Monitor service usage and costs

* **GitHub Environment:**

  * Required for storing:

    * **SQL scripts**
    * **Python code**
  * Project submission via GitHub repository

* **Workflow Environment Configuration:**

  * Code development using:

    * **AWS Glue**
    * **Glue Studio (web-based editors)**
  * Save code from AWS editors to **local GitHub repo**
  * Local Python editors can be used for development
  * **Glue Jobs must be run in AWS Glue environment**

## Project Data

### 📦 **Data Sources (JSON)**

Located in GitHub repo:
🔗 `https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main/project/starter`

* `customer/landing`
* `step_trainer/landing`
* `accelerometer/landing`

---

### 📥 **Download Instructions**

1. Visit GitHub repo: `nd027-Data-Engineering-Data-Lakes-AWS-Exercises`
2. Click **Download ZIP**
3. Extract ZIP file
4. Navigate to `project/starter/`
5. Find JSON files in:

   * `customer/landing`
   * `step_trainer/landing`
   * `accelerometer/landing`

---

### 📊 **Expected Row Counts**

| Table Name              | Expected Rows |
| ----------------------- | ------------- |
| `customer_landing`      | 956           |
| `accelerometer_landing` | 81273         |
| `step_trainer_landing`  | 28680         |

---

### 📁 **1. Customer Records**

**Source:** Fulfillment & STEDI website
**S3 URI:** `s3://cd0030bucket/customers/`
**Fields:**

* `serialnumber`
* `sharewithpublicasofdate`
* `birthday`
* `registrationdate`
* `sharewithresearchasofdate`
* `customername`
* `email`
* `lastupdatedate`
* `phone`
* `sharewithfriendsasofdate`

---

### 📁 **2. Step Trainer Records**

**Source:** Motion sensor
**S3 URI:** `s3://cd0030bucket/step_trainer/`
**Fields:**

* `sensorReadingTime`
* `serialNumber`
* `distanceFromObject`

---

### 📁 **3. Accelerometer Records**

**Source:** Mobile app
**S3 URI:** `s3://cd0030bucket/accelerometer/`
**Fields:**

* `timeStamp`
* `user`
* `x`
* `y`
* `z`

---

### 📄 **README Review**

* Review the GitHub project [README](https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/blob/main/project/README.md) for:

  * Project structure
  * Data context
  * Setup details


## Project Instructions

### **Overview**

Using **AWS Glue**, **AWS S3**, **Python**, and **Apache Spark**, build a **Lakehouse architecture** on AWS that satisfies the following requirements from the STEDI data science team.

Refer to the diagrams below to understand the **workflow** and **data relationships**:

* ![Workflow Flowchart](https://video.udacity-data.com/topher/2023/October/6527a6fc_flowchart/flowchart.jpeg)
* ![Entity Relationship Diagram](https://video.udacity-data.com/topher/2023/October/6527a70f_dataset/dataset.jpeg)

---

### **Part 1: Setup Landing Zone Tables**

#### **1. Simulate Landing Zones**

Create S3 directories for:

* `customer_landing`
* `step_trainer_landing`
* `accelerometer_landing`

Copy sample data to each directory.

#### **2. Create Glue Tables for Each Landing Zone**

Use SQL scripts to define Glue tables:

* `customer_landing.sql`
* `accelerometer_landing.sql`
* `step_trainer_landing.sql`

> 💡 **Submit these scripts to GitHub.**

#### **3. Verify Tables in Athena**

Query each table in **Athena**, and capture screenshots of results:

* `customer_landing.png`
* `accelerometer_landing.png`
* `step_trainer_landing.png`

---

### **Part 2: Create Trusted Zone**

Data sanitization to filter records from customers who agreed to share their data.

#### **1. Customer Trusted Table**

* Sanitize customer records from the landing zone.
* Filter only those who consented to share data (shareWithResearchAsOfDate not null)
* Output to Glue table: **`customer_trusted`**

📄 Script: `customer_trusted.py`
📸 Screenshot: `customer_trusted.png`

#### **2. Accelerometer Trusted Table**

* Sanitize accelerometer records from the landing zone.
* Keep data only for customers in `customer_trusted`.
* Output to Glue table: **`accelerometer_trusted`**

📄 Script: `accelerometer_trusted.py`

---

### **Part 3: Build Curated Customer Table**

Resolve the **serial number defect** in the original fulfillment data.

#### **1. Customers Curated Table**

* Join `customer_trusted` and `accelerometer_trusted`
* Keep only customers who have accelerometer data and gave consent.
* Output to Glue table: **`customers_curated`**

📄 Script: `customers_curated.py`

---

### **Part 4: Process Step Trainer Data**

#### **1. Step Trainer Trusted Table**

* Use **Glue Studio Job**
* Read `step_trainer_landing` (IoT S3 stream)
* Filter based on `customers_curated`
* Output to Glue table: **`step_trainer_trusted`**

📄 Job: `step_trainer_trusted.py`

#### **2. Machine Learning Curated Table**

* Join `step_trainer_trusted` and `accelerometer_trusted` by timestamp
* Filter for customers with consent
* Output to Glue table: **`machine_learning_curated`**

📄 Job: `machine_learning_curated.py`

---

### **Part 5: Validate Results**

Check row counts in each zone as follows:

| **Zone**    | **Table**                  | **Expected Row Count** |
| ----------- | -------------------------- | ---------------------- |
| **Landing** | `customer_landing`         | 956                    |
|             | `accelerometer_landing`    | 81,273                 |
|             | `step_trainer_landing`     | 28,680                 |
| **Trusted** | `customer_trusted`         | 482                    |
|             | `accelerometer_trusted`    | 40,981                 |
|             | `step_trainer_trusted`     | 14,460                 |
| **Curated** | `customers_curated`        | 482                    |
|             | `machine_learning_curated` | 43,681                 |

---

### **Hints & Best Practices**

* ✅ Use **SQL Transform nodes** in Glue Studio wherever possible.
  SQL queries with multiple parent nodes are more reliable than Join nodes.

---

### **Submission Checklist**

✅ Python scripts:

* `customer_trusted.py`
* `accelerometer_trusted.py`
* `customers_curated.py`
* `step_trainer_trusted.py`
* `machine_learning_curated.py`

✅ SQL scripts:

* `customer_landing.sql`
* `accelerometer_landing.sql`
* `step_trainer_landing.sql`

✅ Screenshots:

* `customer_landing.png`
* `accelerometer_landing.png`
* `step_trainer_landing.png`
* `customer_trusted.png`

✅ Project meets all criteria outlined in the **rubric**.


## Troubleshooting

LessonCloud Resources

### I get an Access Denied error when running a Glue Job

If you get the following error:

![Access Denied error when running a Glue Job](https://video.udacity-data.com/topher/2024/June/66755b77_1/1.jpeg)

Access Denied error when running a Glue Job

That means Glue does not have access to your S3 bucket. Make sure the IAM role's **Permissions** and **Trust Relationships** are properly set.

### When joining step_trainer_landing and customer_curated to create step_trainer_trusted, I get zero rows

This problem happens when you perform an inner join between step_trainer_landing and customer_curated **in a Glue job**.

![The output of join transform between step trainer landing and customer curated tables returned zero rows](https://video.udacity-data.com/topher/2024/June/66755ff5_1/1.jpeg)

The transform - join node returned zero rows

This issue is particularly strange because it only occurs in Glue Jobs. In Amazon Athena, the inner join works perfectly fine:

![The result of an SQL query "SELECT * FROM step_trainer_landing s JOIN customer_curated c ON s.serialnumber = c.serialnumber" gives the output of 14,640 rows, which is the correct number of rows.](https://video.udacity-data.com/topher/2024/June/667560f2_2/2.jpeg)

The same inner join produces the correct number of rows

A combination of two events causes this issue:

1. The serialnumber field is not unique in both step_trainer_landing and customer_curated tables.
2. AWS Glue uses Apache Spark, a distributed data processing engine, while Amazon Athena uses Presto, a distributed SQL query engine. They don't handle data differently, especially on complex scenarios such as this one.

The solution here is to avoid using inner join, since it is not appropriate in this case anyway, due to non-unique values. Instead, you should **Select all rows in step_trainer_landing table that also exist IN the customer_curated table.**

### AWS Glue Job does not produce expected output

"expected output" here means the output that you get when you run a query directly in Amazon Athena. For example, getting the machine_learning_curated rows by running this query:

`select *  from accelerometer_trusted a join step_trainer_trusted t on a.timestamp = t.sensorreadingtime;`

The output may differ from that of a Glue Job since the latter uses Spark to join rows, which can behave differently from a database system when joining tables through columns with non-unique values — for example, in this case, there are multiple duplicate timestamps and sensor reading times.

To deal with cases like this, you may try replacing Glue nodes with an SQL Query node.

Also, in general, anytime you make changes, please do the following:

- Before running a Glue Job, **ensure it is saved;** otherwise, the previous version of the Job will be executed.
- Delete the existing table in Amazon Athena.
- Delete existing files in the S3 bucket.

## Project Rubric

---

### Landing Zone

#### Ingest Data from S3 using Glue Studio

* Glue Jobs:

  * `customer_landing_to_trusted.py`
  * `accelerometer_landing_to_trusted.py`
  * `step_trainer_trusted.py`
* Each job has a node connecting to the appropriate S3 bucket (customer, accelerometer, step trainer).

#### Create Glue Tables Manually from JSON

* SQL DDL Scripts:

  * `customer_landing.sql`
  * `accelerometer_landing.sql`
  * `step_trainer_landing.sql`
* Fields must be explicitly typed (avoid using `STRING` for all fields).

#### Query Landing Zone with Athena

* Required screenshots of Athena queries:

  * `customer_landing`: 956 rows

    * Includes rows with blank `shareWithResearchAsOfDate`
  * `accelerometer_landing`: 81,273 rows
  * `step_trainer_landing`: 28,680 rows

---

### Trusted Zone

#### Enable Dynamic Schema Updates in Glue

* Glue job option:

  * `"Create a table in the Data Catalog and update schema/partitions"` = `True`

#### Query Trusted Tables with Athena

* Required screenshots of Athena queries:

  * `customer_trusted`: 482 rows (no blank `shareWithResearchAsOfDate`)
  * `accelerometer_trusted`: 40,981 rows
  * `step_trainer_trusted`: 14,460 rows

* **Stand-out goal counts**:

  * `accelerometer_trusted`: 32,025 rows (filtered by consent date)

#### Filter Protected PII using Spark in Glue

* `customer_landing_to_trusted.py`:

  * Drop rows with null `shareWithResearchAsOfDate`
* Hint: Use `Transform - SQL Query` node for consistency.

#### Join Privacy Tables in Glue

* `accelerometer_landing_to_trusted.py`:

  * Inner join `customer_trusted` and `accelerometer_landing` on `email`
  * Result should include only accelerometer fields

* Cleanup reminder:

  * Delete/recreate S3 and Athena tables when re-running ETLs

---

### Curated Zone

#### Join Trusted Data in Glue

* `customer_trusted_to_curated.py`:

  * Inner join `customer_trusted` and `accelerometer_trusted` on `email`
  * Output should include only customer fields

#### Create Final Curated Data Sets

* `step_trainer_trusted.py`: join `step_trainer_landing` + `customer_curated` by `serialNumber`
* `machine_learning_curated.py`: join `step_trainer_trusted` + `accelerometer_trusted` by timestamps

#### Query Curated Tables with Athena

* Required screenshots:

  * `customer_curated`: 482 rows
  * `machine_learning_curated`: 43,681 rows

* **Stand-out goal counts**:

  * `customer_curated`: 464 rows
  * `machine_learning_curated`: 34,437 rows

#### Hints

* Prefer `Data Catalog` nodes over `S3` nodes for consistency
* Use `Data Preview` with ≥500 rows to validate join results
* `SQL Query` nodes > Join nodes for consistent behavior
* `step_trainer_trusted.py` job may take \~8 minutes to run

---

### Stand-Out Suggestions

#### Filter Accelerometer Data by Consent Date

* Only include readings after the `shareWithResearchAsOfDate`
* Ensures ethical use of data with active consent

#### Anonymize Final Curated Table

* Remove PII like `email`
* Helps ensure GDPR compliance
* Protects from future deletion requests